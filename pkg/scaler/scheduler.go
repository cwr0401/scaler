package scaler

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/AliyunContainerService/scaler/pkg/config"
	"github.com/AliyunContainerService/scaler/pkg/model"
	"github.com/AliyunContainerService/scaler/pkg/platform_client"
	"github.com/AliyunContainerService/scaler/pkg/telemetry"
	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type scheduler struct {
	config         *config.Config
	metaData       *model.Meta
	platformClient platform_client.Client
	mu             sync.Mutex
	wg             sync.WaitGroup
	units          map[string]*Unit
	idleUnit       *list.List

	// Assign 频率
	assignStats requestStatistics

	// Idle 频率
	idleStats requestStatistics
}

type requestStatistics struct {
	Count            uint64
	IntervalDuration []time.Duration
	lastTime         time.Time
}

func NewRequestStatistics() requestStatistics {
	return requestStatistics{
		Count:            0,
		IntervalDuration: make([]time.Duration, 0, 1024),
		lastTime:         time.Now(),
	}
}

func (r *requestStatistics) Inc(now time.Time) {
	if r.Count != 0 {
		r.IntervalDuration = append(r.IntervalDuration, now.Sub(r.lastTime))
	}
	r.Count += 1
	r.lastTime = now
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
		mu:             sync.Mutex{},
		wg:             sync.WaitGroup{},
		units:          make(map[string]*Unit),
		idleUnit:       list.New(),
		assignStats:    NewRequestStatistics(),
		idleStats:      NewRequestStatistics(),
	}
	log.WithField("app", metaData.Key).Info("New scaler is created")
	// log.Infof("New scaler for app: %s is created", metaData.Key)
	scheduler.wg.Add(1)

	go func() {
		defer scheduler.wg.Done()
		scheduler.gcLoop()
		log.WithField("app", metaData.Key).Warnf("Scheduler GC goroutinue is stopped")
		// log.Infof("gc loop 2 for app: %s is stoped", metaData.Key)
	}()

	return scheduler
}

func (s *scheduler) Assign(ctx context.Context, request *pb.AssignRequest) (*pb.AssignReply, error) {
	start := time.Now()
	instanceId := uuid.New().String()

	defer func() {
		log.WithFields(log.Fields{
			"app":         s.metaData.Key,
			"request_id":  request.RequestId,
			"instance_id": instanceId,
		}).Infof("Assign cost %dms", time.Since(start).Milliseconds())
	}()
	log.WithFields(log.Fields{
		"app":        s.metaData.Key,
		"request_id": request.RequestId,
	}).Infof("Scheduler Assign started")

	s.mu.Lock()
	s.assignStats.Inc(start)
	if element := s.idleUnit.Front(); element != nil {
		unit := element.Value.(*Unit)
		unit.SetBusy()
		s.idleUnit.Remove(element)
		s.mu.Unlock()
		instanceId = unit.Instance.Id
		// log.Infof("Assign, request id: %s, instance %s reused", request.RequestId, instanceId)
		log.WithFields(log.Fields{
			"app":         s.metaData.Key,
			"request_id":  request.RequestId,
			"instance_id": instanceId,
		}).Infof("Scheduler Assign reuse instance")
		telemetry.Metrics.SchedulerAssignReuse.WithLabelValues(
			s.metaData.Key,
		).Inc()
		return &pb.AssignReply{
			Status: pb.Status_Ok,
			Assigment: &pb.Assignment{
				RequestId:  request.RequestId,
				MetaKey:    unit.Instance.Meta.Key,
				InstanceId: unit.Instance.Id,
			},
			ErrorMessage: nil,
		}, nil
	}
	s.mu.Unlock()

	//Create new Instance
	resourceConfig := model.SlotResourceConfig{
		ResourceConfig: pb.ResourceConfig{
			MemoryInMegabytes: request.MetaData.MemoryInMb,
		},
	}
	log.WithFields(log.Fields{
		"app":         s.metaData.Key,
		"request_id":  request.RequestId,
		"instance_id": instanceId,
	}).Infof("Scheduler Assign invokes CreateSlot")
	slot, err := s.platformClient.CreateSlot(ctx, request.RequestId, &resourceConfig)
	if err != nil {
		telemetry.Metrics.SchedulerAssignCreateDurations.WithLabelValues(s.metaData.Key, "CreateSlotFailed").Observe(
			float64(time.Since(start).Milliseconds()),
		)
		errorMessage := fmt.Sprintf("create slot failed with: %s", err.Error())
		// log.Infof(errorMessage)
		log.WithFields(log.Fields{
			"app":         s.metaData.Key,
			"request_id":  request.RequestId,
			"instance_id": instanceId,
		}).Errorf("Scheduler Assign %s", errorMessage)
		return nil, status.Errorf(codes.Internal, errorMessage)
	}

	meta := &model.Meta{
		Meta: pb.Meta{
			Key:           request.MetaData.Key,
			Runtime:       request.MetaData.Runtime,
			TimeoutInSecs: request.MetaData.TimeoutInSecs,
		},
	}
	log.WithFields(log.Fields{
		"app":         s.metaData.Key,
		"request_id":  request.RequestId,
		"instance_id": instanceId,
	}).Infof("Scheduler Assign invokes Init")
	instance, err := s.platformClient.Init(ctx, request.RequestId, instanceId, slot, meta)
	if err != nil {
		telemetry.Metrics.SchedulerAssignCreateDurations.WithLabelValues(s.metaData.Key, "InitFailed").Observe(
			float64(time.Since(start).Milliseconds()),
		)
		errorMessage := fmt.Sprintf("create instance failed with: %s", err.Error())
		// log.Infof(errorMessage)
		log.WithFields(log.Fields{
			"app":         s.metaData.Key,
			"request_id":  request.RequestId,
			"instance_id": instanceId,
		}).Errorf("Scheduler Assign %s", errorMessage)
		return nil, status.Errorf(codes.Internal, errorMessage)
	}

	//add new instance
	s.mu.Lock()
	unit := NewUnit(instance, s.config.CostFluctuation)
	unit.SetBusy()
	s.units[instance.Id] = unit

	s.mu.Unlock()
	// log.Infof("request id: %s, instance %s for app %s is created, init latency: %dms", request.RequestId, instance.Id, instance.Meta.Key, instance.InitDurationInMs)
	log.WithFields(log.Fields{
		"request_id":  request.RequestId,
		"instance_id": instance.Id,
		"app":         instance.Meta.Key,
	}).Infof("Scheduler Assign create instance init latency: %dms", instance.InitDurationInMs)
	telemetry.Metrics.SchedulerAssignCreateDurations.WithLabelValues(s.metaData.Key, "InitFailed").Observe(
		float64(time.Since(start).Milliseconds()),
	)
	return &pb.AssignReply{
		Status: pb.Status_Ok,
		Assigment: &pb.Assignment{
			RequestId:  request.RequestId,
			MetaKey:    instance.Meta.Key,
			InstanceId: instance.Id,
		},
		ErrorMessage: nil,
	}, nil
}

func (s *scheduler) Idle(ctx context.Context, request *pb.IdleRequest) (*pb.IdleReply, error) {
	start := time.Now()
	if request.Assigment == nil {
		telemetry.Metrics.SchedulerIdle.WithLabelValues(s.metaData.Key, "InvalidArgument").Inc()
		errorMessage := fmt.Sprintf("assignment is nil")
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
	//log.Infof("Idle, request id: %s", request.Assigment.RequestId)

	slotId := ""
	if request.Result != nil && request.Result.NeedDestroy != nil && *request.Result.NeedDestroy {
		needDestroy = true
	}
	defer func() {
		if needDestroy {
			s.deleteSlot(ctx, request.Assigment.RequestId, slotId, instanceId, request.Assigment.MetaKey, "bad instance")
		}
	}()
	log.WithFields(log.Fields{
		"app":        s.metaData.Key,
		"request_id": request.Assigment.RequestId,
	}).Infof("Scheduler Idle started")
	s.mu.Lock()
	s.idleStats.Inc(start)
	defer s.mu.Unlock()
	if unit := s.units[instanceId]; unit != nil {
		slotId = unit.Instance.Slot.Id
		if needDestroy {
			action = "Destroy"
			log.WithFields(log.Fields{
				"app":         s.metaData.Key,
				"request_id":  request.Assigment.RequestId,
				"instance_id": instanceId,
			}).Infof("Scheduler Idle instance need be destroy")
			delete(s.units, instanceId)
			unit.Status = UnitDestroy
			return reply, nil
		}

		if !unit.Instance.Busy {
			log.WithFields(log.Fields{
				"app":         s.metaData.Key,
				"request_id":  request.Assigment.RequestId,
				"instance_id": instanceId,
			}).Warnf("Scheduler Idle instance already freed")
			action = "AlreadyFreed"
			return reply, nil
		}
		action = "Release"
		unit.SetIdle()
		s.idleUnit.PushFront(unit)
	} else {
		action = "NotFound"
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("request id %s, instance %s not found", request.Assigment.RequestId, instanceId))
	}

	return &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}, nil
}

func (s *scheduler) deleteSlot(ctx context.Context, requestId, slotId, instanceId, metaKey, reason string) {
	log.Infof("start delete Instance %s (Slot: %s) of app: %s", instanceId, slotId, metaKey)
	if err := s.platformClient.DestroySLot(ctx, requestId, slotId, reason); err != nil {
		log.Infof("delete Instance %s (Slot: %s) of app: %s failed with: %s", instanceId, slotId, metaKey, err.Error())
	}
}

func (s *scheduler) gcLoop() {
	log.Infof("gc loop for app: %s is started", s.metaData.Key)
	ticker := time.NewTicker(1 * time.Millisecond)
	for range ticker.C {
		s.mu.Lock()
		// log.Infof("gc loop for app: %s is started, TotalInstance %d TotalIdleInstance %d", s.metaData.Key, len(s.units), s.idleUnit.Len())
		for e := s.idleUnit.Front(); e != nil; e = e.Next() {
			unit := e.Value.(*Unit)
			unit.SetNeedDestroy()
			if unit.Status == UnitNeedDestroy {
				// log.Infof("gc loop for app: %s, Instance %s start destroy.", s.metaData.Key, unit.Id())
				s.idleUnit.Remove(e)
				delete(s.units, unit.Instance.Id)
				go func() {
					log.Infof("Instance %s Delete(%s)", unit.Id(), unit.DestroyReason)
					ctx := context.Background()
					ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
					defer cancel()
					s.deleteSlot(ctx, uuid.NewString(), unit.SlotId(), unit.Id(), unit.MetaKey(), unit.DestroyReason)
				}()
				unit.Status = UnitDestroy
			}
		}
		s.mu.Unlock()
	}
}

func (s *scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		TotalInstance:     len(s.units),
		TotalIdleInstance: s.idleUnit.Len(),
	}
}
