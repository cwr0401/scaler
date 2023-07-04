package scaler

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/AliyunContainerService/scaler/pkg/config"
	"github.com/AliyunContainerService/scaler/pkg/model"
	"github.com/AliyunContainerService/scaler/pkg/platform_client"
	"github.com/AliyunContainerService/scaler/pkg/telemetry"
	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/sirupsen/logrus"
)

type scheduler struct {
	config         *config.Config
	metaData       *model.Meta
	platformClient platform_client.Client
	rw             sync.RWMutex
	wg             sync.WaitGroup

	units    map[string]*Unit
	idleUnit *list.List
	idleChan chan struct{}

	maxIdleUnit int
	minIdleUnit int
}

func NewScheduler(metaData *model.Meta, config *config.Config) Scaler {
	client, err := platform_client.New(config.ClientAddr)
	if err != nil {
		log.WithField("app", metaData.Key).Fatalf("client init with error: %s", err.Error())
	}

	scheduler := &scheduler{
		config:         config,
		metaData:       metaData,
		platformClient: client,
		rw:             sync.RWMutex{},
		wg:             sync.WaitGroup{},

		units:    make(map[string]*Unit),
		idleUnit: list.New(),

		idleChan:    make(chan struct{}, 5),
		maxIdleUnit: 5,
		minIdleUnit: 1,
	}

	log.WithField("app", metaData.Key).Info("New scaler is created")
	telemetry.Metrics.SchedulerUnitSlotResources.WithLabelValues(metaData.Key).Set(float64(metaData.GetMemoryInMb()))
	scheduler.wg.Add(1)

	// go func() {
	// 	defer scheduler.wg.Done()
	// 	scheduler.gcLoop()
	// 	log.WithField("app", metaData.Key).Warnf("Scheduler GC goroutinue is stopped")
	// 	// log.Infof("gc loop 2 for app: %s is stoped", metaData.Key)
	// }()

	return scheduler
}

func (s *scheduler) Assign(ctx context.Context, request *pb.AssignRequest) (*pb.AssignReply, error) {
	start := time.Now()
	log.WithFields(log.Fields{
		"app":        s.metaData.Key,
		"request_id": request.RequestId,
	}).Infof("Scheduler Assign started")

	maxWaitTime, err := time.ParseDuration(fmt.Sprintf("%ds", request.MetaData.TimeoutInSecs))
	if err != nil {
		maxWaitTime = 1 * time.Second
	}

	unit, err := s.GetUnit(ctx, request.RequestId, maxWaitTime)

	if err != nil {
		log.WithFields(log.Fields{
			"app":        s.metaData.Key,
			"request_id": request.RequestId,
		}).Infof("Assign instance error: %s", err.Error())
		return nil, err
	}

	log.WithFields(log.Fields{
		"app":         s.metaData.Key,
		"request_id":  request.RequestId,
		"instance_id": unit.InstanceId,
	}).Infof("Assign cost %dms", time.Since(start).Milliseconds())

	return &pb.AssignReply{
		Status: pb.Status_Ok,
		Assigment: &pb.Assignment{
			RequestId:  request.RequestId,
			MetaKey:    unit.MetaKey,
			InstanceId: unit.InstanceId,
		},
		ErrorMessage: nil,
	}, nil

}

func (s *scheduler) ReuseUnit(requestId string) *Unit {
	s.rw.Lock()
	if element := s.idleUnit.Front(); element != nil {
		unit := element.Value.(*Unit)
		s.idleUnit.Remove(element)
		err := unit.SetUnixExecutingStatus()
		s.rw.Unlock()

		if err != nil {
			return nil
		}
		log.WithFields(log.Fields{
			"app":         s.metaData.Key,
			"request_id":  requestId,
			"instance_id": unit.InstanceId,
		}).Infof("Scheduler Assign reuse instance")

		telemetry.Metrics.SchedulerAssignReuse.WithLabelValues(
			s.metaData.Key,
		).Inc()

		s.rw.Unlock()
		return unit
	}

	return nil
}

func (s *scheduler) CreateUnit(ctx context.Context, requestId string) (*Unit, error) {
	// 等待超时，直接创建
	//Create new Instance
	instanceId := uuid.New().String()

	resourceConfig := model.SlotResourceConfig{
		ResourceConfig: pb.ResourceConfig{
			MemoryInMegabytes: s.metaData.MemoryInMb,
		},
	}
	log.WithFields(log.Fields{
		"app":         s.metaData.Key,
		"request_id":  requestId,
		"instance_id": instanceId,
	}).Infof("Scheduler Assign invokes CreateSlot")
	start := time.Now()
	slot, err := s.platformClient.CreateSlot(ctx, requestId, &resourceConfig)

	if err != nil {
		telemetry.Metrics.SchedulerAssignCreateDurations.WithLabelValues(s.metaData.Key, "CreateSlotFailed").Observe(
			float64(time.Since(start).Milliseconds()),
		)
		errorMessage := fmt.Sprintf("create slot failed with: %s", err.Error())
		log.WithFields(log.Fields{
			"app":         s.metaData.Key,
			"request_id":  requestId,
			"instance_id": instanceId,
		}).Errorf("Scheduler Assign %s", errorMessage)
		return nil, status.Errorf(codes.Internal, errorMessage)
	}
	meta := &model.Meta{
		Meta: pb.Meta{
			Key:           s.metaData.Key,
			Runtime:       s.metaData.Runtime,
			TimeoutInSecs: s.metaData.TimeoutInSecs,
		},
	}
	log.WithFields(log.Fields{
		"app":         s.metaData.Key,
		"request_id":  requestId,
		"instance_id": instanceId,
	}).Infof("Scheduler Assign invokes Init")

	instance, err := s.platformClient.Init(ctx, requestId, instanceId, slot, meta)

	if err != nil {
		telemetry.Metrics.SchedulerAssignCreateDurations.WithLabelValues(s.metaData.Key, "InitFailed").Observe(
			float64(time.Since(start).Milliseconds()),
		)
		errorMessage := fmt.Sprintf("create instance failed with: %s", err.Error())
		// log.Infof(errorMessage)
		log.WithFields(log.Fields{
			"app":         s.metaData.Key,
			"request_id":  requestId,
			"instance_id": instanceId,
		}).Errorf("Scheduler Assign %s", errorMessage)
		return nil, status.Errorf(codes.Internal, errorMessage)
	}

	unit := NewUnit(instance, s.config.CostFluctuation)

	s.rw.Lock()
	unit.SetUnixExecutingStatus()
	s.units[instance.Id] = unit
	s.rw.Unlock()

	return unit, nil
}

func (s *scheduler) GetUnit(ctx context.Context, requestId string, waitTime time.Duration) (*Unit, error) {
	// 复用
	unit := s.ReuseUnit(requestId)
	if unit != nil {
		return unit, nil
	}

	status := s.Stats()

	if status.TotalInstance > 0 {
		// 等待空闲
		select {
		case <-s.idleChan:
			unit := s.ReuseUnit(requestId)
			if unit != nil {
				return unit, nil
			}
		case <-time.After(waitTime):
			log.WithFields(log.Fields{
				"app":        s.metaData.Key,
				"request_id": requestId,
			}).Infof("Wait for the idle instance for more than %s, Create new instance", waitTime)
		}
	}

	return s.CreateUnit(ctx, requestId)
}

func (s *scheduler) Idle(ctx context.Context, request *pb.IdleRequest) (*pb.IdleReply, error) {
	start := time.Now()
	if request.Assigment == nil {
		telemetry.Metrics.SchedulerIdle.WithLabelValues(s.metaData.Key, "InvalidArgument").Inc()
		errorMessage := "assignment is nil"
		log.WithFields(log.Fields{
			"app": s.metaData.Key,
		}).Errorf("Scheduler Idle %s", errorMessage)
		return nil, status.Errorf(codes.InvalidArgument, errorMessage)
	}
	reply := &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}

	instanceId := request.Assigment.InstanceId
	needDestroy := false
	var instanceCreateSlotTime time.Time

	action := ""

	defer func() {
		durationInMs := time.Since(start).Milliseconds()
		log.WithFields(log.Fields{
			"app":         s.metaData.Key,
			"request_id":  request.Assigment.RequestId,
			"instance_id": instanceId,
			"action":      action,
		}).Infof("Scheduler Idle, cost %dus", durationInMs)
		telemetry.Metrics.SchedulerIdle.WithLabelValues(s.metaData.Key, action).Inc()
	}()

	slotId := ""
	if request.Result != nil && request.Result.NeedDestroy != nil && *request.Result.NeedDestroy {
		needDestroy = true
	}
	defer func() {
		if needDestroy {
			s.deleteSlot(ctx, request.Assigment.RequestId, slotId, instanceId, request.Assigment.MetaKey, "bad instance", instanceCreateSlotTime)
		}
	}()
	log.WithFields(log.Fields{
		"app":        s.metaData.Key,
		"request_id": request.Assigment.RequestId,
	}).Infof("Scheduler Idle started")

	s.rw.Lock()
	defer s.rw.Unlock()

	if unit := s.units[instanceId]; unit != nil {
		slotId = unit.SlotId
		instanceCreateSlotTime = unit.createSlotTime
		if needDestroy {
			action = "Destroy"
			log.WithFields(log.Fields{
				"app":         s.metaData.Key,
				"request_id":  request.Assigment.RequestId,
				"instance_id": instanceId,
			}).Infof("Scheduler Idle instance need be destroy")
			delete(s.units, instanceId)
			unit.status = UnitDestroyed
			return reply, nil
		}

		if unit.status != UnitExecuting {
			log.WithFields(log.Fields{
				"app":         s.metaData.Key,
				"request_id":  request.Assigment.RequestId,
				"instance_id": instanceId,
			}).Warnf("Scheduler Idle instance already freed")
			action = "AlreadyFreed"
			return reply, nil
		}
		action = "Release"
		err := unit.SetUnixIdleStatus()
		if err != nil {
			return reply, nil
		}

		select {
		case s.idleChan <- struct{}{}:
			s.idleUnit.PushFront(unit)
		default:
			needDestroy = true
			action = "Destroy"
			delete(s.units, instanceId)
			unit.status = UnitDestroyed
			log.WithFields(log.Fields{
				"app":         s.metaData.Key,
				"request_id":  request.Assigment.RequestId,
				"instance_id": instanceId,
			}).Infof("Idle Channel is full, destroy instance")
		}

	} else {
		action = "NotFound"
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("request id %s, instance %s not found", request.Assigment.RequestId, instanceId))
	}

	return &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}, nil

}

func (s *scheduler) deleteSlot(ctx context.Context, requestId, slotId, instanceId, metaKey, reason string, createTime time.Time) {
	aliveTime := time.Since(createTime).Milliseconds()

	// log.Infof("start delete Instance %s (Slot: %s) of app: %s", instanceId, slotId, metaKey)
	log.WithFields(log.Fields{
		"app":         s.metaData.Key,
		"request_id":  requestId,
		"instance_id": instanceId,
	}).Infof("Start delete Instance(Slot: %s), Alive time %dms", slotId, aliveTime)

	telemetry.Metrics.SchedulerUnitDurations.WithLabelValues(s.metaData.Key, "all").Observe(
		float64(aliveTime),
	)

	if err := s.platformClient.DestroySLot(ctx, requestId, slotId, reason); err != nil {
		// log.Infof("delete Instance %s (Slot: %s) of app: %s failed with: %s", instanceId, slotId, metaKey, err.Error())
		log.WithFields(log.Fields{
			"app":         s.metaData.Key,
			"request_id":  requestId,
			"instance_id": instanceId,
		}).Infof("Delete Instance(Slot: %s) failed with: %s", slotId, err.Error())
	}
}

func (s *scheduler) gcLoop() {
	log.Infof("gc loop for app: %s is started", s.metaData.Key)
	ticker := time.NewTicker(1 * time.Millisecond)
	for range ticker.C {
		stats := s.Stats()

		if stats.TotalInstance <= s.minIdleUnit {
			s.rw.Lock()
			for e := s.idleUnit.Front(); e != nil; e = e.Next() {
				unit := e.Value.(*Unit)

				idleDuration := time.Since(unit.lastIdleTime)
				maxIdleTimeInMs := int64(unit.TimeCostFluctuation() * 50)
				if idleDuration.Milliseconds() > maxIdleTimeInMs {
					unit.status = UnitNeedDestroy
					unit.destroyReason = fmt.Sprintf("Idle duration: %s, max idle duration: %dms", idleDuration, maxIdleTimeInMs)
				}

				if unit.status == UnitNeedDestroy {
					// log.Infof("gc loop for app: %s, Instance %s start destroy.", s.metaData.Key, unit.Id())
					s.idleUnit.Remove(e)
					delete(s.units, unit.InstanceId)
					go func() {
						log.Infof("Instance %s Delete(%s)", unit.InstanceId, unit.DestroyReason())
						ctx := context.Background()
						ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
						defer cancel()
						s.deleteSlot(ctx, uuid.NewString(), unit.SlotId, unit.InstanceId, unit.MetaKey, unit.DestroyReason(), unit.createSlotTime)
					}()
					unit.status = UnitDestroyed
				}
			}
			s.rw.Unlock()
			continue
		}

		s.rw.Lock()
		// log.Infof("gc loop for app: %s is started, TotalInstance %d TotalIdleInstance %d", s.metaData.Key, len(s.units), s.idleUnit.Len())
		for e := s.idleUnit.Front(); e != nil; e = e.Next() {
			unit := e.Value.(*Unit)
			err := unit.SetNeedDestroyStatus()
			if err != nil {
				continue
			}
			if unit.status == UnitNeedDestroy {
				// log.Infof("gc loop for app: %s, Instance %s start destroy.", s.metaData.Key, unit.Id())
				s.idleUnit.Remove(e)
				delete(s.units, unit.InstanceId)
				go func() {
					log.Infof("Instance %s Delete(%s)", unit.InstanceId, unit.DestroyReason())
					ctx := context.Background()
					ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
					defer cancel()
					s.deleteSlot(ctx, uuid.NewString(), unit.SlotId, unit.InstanceId, unit.MetaKey, unit.DestroyReason(), unit.createSlotTime)
				}()
				unit.status = UnitDestroyed
			}
		}
		s.rw.Unlock()
	}
}

func (s *scheduler) Stats() Stats {
	s.rw.RLock()
	defer s.rw.RUnlock()
	return Stats{
		TotalInstance:     len(s.units),
		TotalIdleInstance: s.idleUnit.Len(),
	}
}
