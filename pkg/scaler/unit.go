package scaler

import (
	"errors"
	"fmt"
	"time"

	"github.com/AliyunContainerService/scaler/pkg/model"

	log "github.com/sirupsen/logrus"
)

type UnitStatus uint8

const (
	UnitInitDone UnitStatus = iota
	UnitExecuting
	UnitIdle
	UnitNeedDestroy
	UnitDestroyed
)

type Unit struct {
	// 实例
	InstanceId string
	SlotId     string
	MetaKey    string

	// 实例状态
	status        UnitStatus
	destroyReason string

	// 成本
	timeCost       uint64
	timeCostFactor float64

	// 时间
	createSlotTime     time.Time
	lastExecutionTime  time.Time
	lastIdleTime       time.Time
	executionDurations []time.Duration
	destroyTime        time.Time

	instance *model.Instance
}

func NewUnit(i *model.Instance, f float64) *Unit {
	return &Unit{
		InstanceId: i.Id,
		SlotId:     i.Slot.Id,
		MetaKey:    i.Meta.Key,
		status:     UnitInitDone,
		// memoryInMegabytes:  i.Slot.ResourceConfig.MemoryInMegabytes,
		timeCost:           i.Slot.CreateDurationInMs + uint64(i.InitDurationInMs),
		timeCostFactor:     f,
		createSlotTime:     time.UnixMilli(int64(i.Slot.CreateTime)),
		executionDurations: []time.Duration{},

		instance: i,
	}

}

func (u *Unit) Status() UnitStatus {
	if u.status == UnitDestroyed {
		log.WithFields(log.Fields{
			"app":         u.MetaKey,
			"instance_id": u.InstanceId,
			"slot_id":     u.SlotId,
		}).Errorf("Instance has been destroyed, cannot invoke it anymore(destory reason: %v)", u.destroyReason)
	}
	return u.status
}

func (u *Unit) DestroyReason() string {
	switch u.status {
	case UnitDestroyed, UnitNeedDestroy:
		return u.destroyReason
	default:
		return ""
	}
}

func (u *Unit) TimeCost() uint64 {
	return u.timeCost
}

func (u *Unit) TimeCostFluctuation() float64 {
	return u.timeCostFactor * float64(u.timeCost)
}

func (u *Unit) SetUnixExecutingStatus() error {
	switch u.status {
	case UnitInitDone:
		u.status = UnitExecuting
		u.instance.Busy = true
		u.lastExecutionTime = time.Now()
	case UnitExecuting:
		log.WithFields(log.Fields{
			"app":         u.MetaKey,
			"instance_id": u.InstanceId,
			"slot_id":     u.SlotId,
		}).Errorf("SetUnixExecutingStatus error, unit is already executing")
		return errors.New("unit is already executing")
	case UnitIdle:
		u.status = UnitExecuting
		u.instance.Busy = true
		u.lastExecutionTime = time.Now()
	case UnitNeedDestroy:
		u.status = UnitExecuting
		u.instance.Busy = true
		u.lastExecutionTime = time.Now()
		log.WithFields(log.Fields{
			"app":         u.MetaKey,
			"instance_id": u.InstanceId,
			"slot_id":     u.SlotId,
		}).Infof("Reuse an need destroy unit")
	case UnitDestroyed:
		log.WithFields(log.Fields{
			"app":         u.MetaKey,
			"instance_id": u.InstanceId,
			"slot_id":     u.SlotId,
		}).Errorf("SetUnixExecutingStatus error, unit has been destroyed")
		return fmt.Errorf("unit is already destroyed as %s", u.destroyTime)
	default:
		return errors.New("invalid status")
	}
	return nil
}

func (u *Unit) SetUnixIdleStatus() error {
	switch u.status {
	case UnitInitDone:
		u.status = UnitIdle
		u.instance.Busy = false
		u.lastIdleTime = time.Now()
		log.WithFields(log.Fields{
			"app":         u.MetaKey,
			"instance_id": u.InstanceId,
			"slot_id":     u.SlotId,
		}).Errorf("SetUnixIdleStatus error, unit not executing")
	case UnitExecuting:
		u.status = UnitIdle
		u.instance.Busy = false
		u.lastIdleTime = time.Now()
		u.executionDurations = append(u.executionDurations, u.lastIdleTime.Sub(u.lastExecutionTime))
	case UnitIdle:
		log.WithFields(log.Fields{
			"app":         u.MetaKey,
			"instance_id": u.InstanceId,
			"slot_id":     u.SlotId,
		}).Errorf("SetUnixIdleStatus error, unit has already idle")
	case UnitNeedDestroy:
		log.WithFields(log.Fields{
			"app":         u.MetaKey,
			"instance_id": u.InstanceId,
			"slot_id":     u.SlotId,
		}).Errorf("SetUnixIdleStatus error, unit need destroy")
	case UnitDestroyed:
		log.WithFields(log.Fields{
			"app":         u.MetaKey,
			"instance_id": u.InstanceId,
			"slot_id":     u.SlotId,
		}).Errorf("SetUnixIdleStatus error, unit has been destroyed")
		return fmt.Errorf("unit is already destroyed as %s", u.destroyTime)
	default:
		return errors.New("invalid status")
	}
	return nil
}

func (u *Unit) SetNeedDestroyStatus() error {
	switch u.status {
	case UnitInitDone:
		// log.WithFields(log.Fields{
		// 	"app":         u.MetaKey,
		// 	"instance_id": u.InstanceId,
		// 	"slot_id":     u.SlotId,
		// }).Errorf("")

	case UnitExecuting:
		return fmt.Errorf("SetNeedDestroyStatus error, unit is in execution state")
	case UnitIdle:
		idleDuration := time.Since(u.lastIdleTime)
		if idleDuration.Milliseconds() > int64(u.TimeCostFluctuation()) {
			u.status = UnitNeedDestroy
			u.destroyReason = fmt.Sprintf("Idle duration: %s, max cost duration: %dms", idleDuration, int64(u.TimeCostFluctuation()))
		}
	case UnitNeedDestroy:
	case UnitDestroyed:
	default:
		return errors.New("invalid status")
	}
	return nil
}
