package scaler

import (
	"fmt"
	"log"
	"time"

	"github.com/AliyunContainerService/scaler/pkg/model"
)

type Unit struct {
	Instance        *model.Instance
	GCQueue         chan struct{}
	NeedDestroy     bool
	DestroyReason   string
	CostFluctuation float64
}

func NewUnit(i *model.Instance, q chan struct{}, f float64) *Unit {
	return &Unit{
		Instance:        i,
		GCQueue:         q,
		CostFluctuation: f,
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

func (i *Unit) setNeedDestroy(maxIdleTimeInMs int64) {
	idleDuration := time.Since(i.Instance.LastIdleTime)
	i.NeedDestroy = (idleDuration.Milliseconds() > maxIdleTimeInMs)
	i.DestroyReason = fmt.Sprintf("Idle duration: %s, max idle duration: %dms", idleDuration, maxIdleTimeInMs)
}

func (i *Unit) ColdStartCost() uint64 {
	return i.Instance.Slot.CreateDurationInMs + uint64(i.Instance.InitDurationInMs)
}

func (i *Unit) gcLoop() {
	log.Printf("gc loop for Meta: %s Unit: %s is started", i.MetaKey(), i.Id())
	// 豪秒级别
	ticker := time.NewTicker(1 * time.Millisecond)
	maxIdleTime := int64((1.0 + i.CostFluctuation) * float64(i.ColdStartCost()))
	for range ticker.C {
		if !i.Instance.Busy {
			i.setNeedDestroy(maxIdleTime)
			if i.NeedDestroy {
				log.Printf("gc loop for Meta: %s, Instance %s need destroy.\n", i.MetaKey(), i.Id())
				i.GCQueue <- struct{}{}
			}
		}
	}
}
