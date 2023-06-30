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

package main

import (
	"log"
	"net"

	"net/http"
	_ "net/http/pprof"

	"google.golang.org/grpc"

	"github.com/AliyunContainerService/scaler/pkg/server"
	"github.com/AliyunContainerService/scaler/pkg/telemetry"
	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	lis, err := net.Listen("tcp", ":9001")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer(grpc.MaxConcurrentStreams(1000))
	pb.RegisterScalerServer(s, server.New())
	log.Printf("server listening at %v", lis.Addr())

	// Expose /metrics HTTP endpoint using the created custom registry.
	http.Handle(
		"/metrics", promhttp.HandlerFor(
			telemetry.PromReg,
			promhttp.HandlerOpts{
				EnableOpenMetrics: true,
			}),
	)

	// metrics
	go func() {
		log.Printf("http server listening at %v", "0.0.0.0:9002")
		err := http.ListenAndServe("0.0.0.0:9002", nil)
		if err != nil {
			log.Fatalf("failed to http listen: %v", err)
		}
	}()

	// Add go runtime metrics and process collectors.

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
