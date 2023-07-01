/*
Copyright 2023 The Alibaba Cloud Serverless Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package manager

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/AliyunContainerService/scaler/pkg/config"
	"github.com/AliyunContainerService/scaler/pkg/model"
	"github.com/AliyunContainerService/scaler/pkg/scaler"
	"github.com/AliyunContainerService/scaler/pkg/telemetry"
)

type Manager struct {
	rw         sync.RWMutex
	schedulers map[string]scaler.Scaler
	config     *config.Config
}

func New(config *config.Config) *Manager {
	return &Manager{
		rw:         sync.RWMutex{},
		schedulers: make(map[string]scaler.Scaler),
		config:     config,
	}
}

func (m *Manager) GetOrCreate(metaData *model.Meta) scaler.Scaler {
	start := time.Now()
	m.rw.RLock()
	if scheduler := m.schedulers[metaData.Key]; scheduler != nil {
		m.rw.RUnlock()
		return scheduler
	}
	m.rw.RUnlock()

	m.rw.Lock()
	if scheduler := m.schedulers[metaData.Key]; scheduler != nil {
		m.rw.Unlock()
		return scheduler
	}
	log.Infof("Create new scaler for app %s", metaData.Key)
	// scheduler := scaler.New(metaData, m.config)
	scheduler := scaler.NewScheduler(metaData, m.config)
	m.schedulers[metaData.Key] = scheduler
	m.rw.Unlock()
	// log.Infof("Create Scheduler %s", time.Since(start))
	telemetry.Metrics.CreateSchedulerDurations.Observe(float64(time.Since(start).Microseconds()))
	return scheduler
}

func (m *Manager) Get(metaKey string) (scaler.Scaler, error) {
	m.rw.RLock()
	defer m.rw.RUnlock()
	if scheduler := m.schedulers[metaKey]; scheduler != nil {
		return scheduler, nil
	}
	return nil, fmt.Errorf("scaler of app: %s not found", metaKey)
}
