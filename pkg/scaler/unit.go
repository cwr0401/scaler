package scaler

import (
	"errors"
	"fmt"
	"time"

	"github.com/AliyunContainerService/scaler/pkg/model"
)

type UnitStatus uint8

const (
	UnitInit UnitStatus = iota
	UnitExecution
	UnitIdle
	UnitNeedDestroy
	UnitDestroy
)

type Unit struct {
	Instance *model.Instance

	// NeedDestroy     bool
	DestroyReason   string
	CostFluctuation float64
	Status          UnitStatus
}

func NewUnit(i *model.Instance, f float64) *Unit {
	return &Unit{
		Instance:        i,
		CostFluctuation: f,
		Status:          UnitInit,
	}

}

func (i *Unit) Id() string {
	return i.Instance.Id
}

func (i *Unit) SlotId() string {
	return i.Instance.Slot.Id
}

func (i *Unit) MetaKey() string {
	return i.Instance.Meta.Key
}

func (i *Unit) SetNeedDestroy() {
	if i.Status == UnitIdle {
		maxIdleTimeInMs := int64((1.0 + i.CostFluctuation) * float64(i.ColdStartCost()))
		idleDuration := time.Since(i.Instance.LastIdleTime)
		if idleDuration.Milliseconds() > maxIdleTimeInMs {
			i.DestroyReason = fmt.Sprintf("Idle duration: %s, max idle duration: %dms", idleDuration, maxIdleTimeInMs)
			i.Status = UnitNeedDestroy
		}
	}
}

func (i *Unit) SetBusy() error {
	switch i.Status {
	case UnitInit, UnitIdle, UnitNeedDestroy:
		i.Status = UnitExecution
		i.Instance.Busy = true
	case UnitDestroy:
		return errors.New("Unit: instance has been destoryed")
	case UnitExecution:
		return errors.New("Unit: instance executing request")
	}
	return nil
}

func (i *Unit) SetIdle() error {
	switch i.Status {
	case UnitInit, UnitExecution, UnitNeedDestroy:
		i.Status = UnitIdle
		i.Instance.Busy = false
		i.Instance.LastIdleTime = time.Now()
	case UnitDestroy:
		return errors.New("Unit: instance has been destoryed")
	case UnitIdle:
		return errors.New("Unit: instance already freed")
	}
	return nil
}

func (i *Unit) ColdStartCost() uint64 {
	return i.Instance.Slot.CreateDurationInMs + uint64(i.Instance.InitDurationInMs)
}

// func (i *Unit) gcLoop() {
// 	log.Printf("gc loop for Meta %s Unit %s is started", i.MetaKey(), i.Id())
// 	// 豪秒级别
// 	ticker := time.NewTicker(1 * time.Millisecond)
// 	maxIdleTime := int64((1.0 + i.CostFluctuation) * float64(i.ColdStartCost()))
// 	for range ticker.C {
// 		if i.Instance.Busy {
// 			continue
// 		}
// 		switch i.Status {
// 		case UnitInit:
// 		case UnitExecution:
// 		case UnitNeedDestroy:
// 			continue
// 		case UnitDestroy:
// 			log.Printf(" %s Unit %s is stopped", i.MetaKey(), i.Id())
// 			return
// 		case UnitIdle:
// 			i.setNeedDestroy(maxIdleTime)
// 			if i.Status == UnitNeedDestroy {
// 				log.Printf("App %s Unit %s need destroy because %s.\n", i.MetaKey(), i.Id(), i.DestroyReason)
// 				select {
// 				case i.GCQueue <- struct{}{}:
// 					log.Printf("App %s Unit %s send a gc signal.", i.MetaKey(), i.Id())
// 				default:
// 				}

// 			}
// 		}
// 	}
// }
