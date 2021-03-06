/*
Copyright 2016 The Kubernetes Authors.

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

package factory

import (
	"fmt"

	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
)

var factories = map[string]StorageFactoryFunc{
	storagebackend.StorageTypeETCD2: newETCD2Storage,
	storagebackend.StorageTypeETCD3: newETCD3Storage,
	storagebackend.StorageTypeUnset: newETCD3Storage,
}

var healthChecks = map[string]StorageHealthCheckFunc{
	storagebackend.StorageTypeETCD2: newETCD2HealthCheck,
	storagebackend.StorageTypeETCD3: newETCD3HealthCheck,
	storagebackend.StorageTypeUnset: errHealthCheck,
}

func errHealthCheck(c storagebackend.Config) (func() error, error) {
	return nil, fmt.Errorf("unknown storage type: %s", c.Type)
}

func Register(name string, factory StorageFactoryFunc, healthCheck StorageHealthCheckFunc) {
	factories[name] = factory
	healthChecks[name] = healthCheck
}

// DestroyFunc is to destroy any resources used by the storage returned in Create() together.
type DestroyFunc func()
type StorageFactoryFunc func(c storagebackend.Config) (storage.Interface, DestroyFunc, error)
type StorageHealthCheckFunc func(c storagebackend.Config) (func() error, error)

// Create creates a storage backend based on given config.
func Create(c storagebackend.Config) (storage.Interface, DestroyFunc, error) {
	factory, ok := factories[c.Type]
	if !ok {
		return nil, nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
	return factory(c)
}

// CreateHealthCheck creates a healthcheck function based on given config.
func CreateHealthCheck(c storagebackend.Config) (func() error, error) {
	check, ok := healthChecks[c.Type]
	if !ok {
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
	return check(c)
}
