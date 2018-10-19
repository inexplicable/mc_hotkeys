package model

import (
	"hash"
)

type Bucketing struct {
	instances []interface{}
	hashing   func() hash.Hash32
	buckets   uint32
}

func (bucketing *Bucketing) Pick(key []byte) interface{} {

	hashing := bucketing.hashing()
	if _, err := hashing.Write(key); err == nil {
		return bucketing.instances[int(hashing.Sum32()%bucketing.buckets)]
	}

	return bucketing.instances[0]
}

func (bucketing *Bucketing) Each(apply func(interface{})) {
	for _, i := range bucketing.instances {
		apply(i)
	}
}

func NewBucketing(new func() interface{}, hashing func() hash.Hash32, buckets uint32) *Bucketing {

	instances := make([]interface{}, 0, buckets)
	for b := 0; b < int(buckets); b++ {
		instances = append(instances, new())
	}
	return &Bucketing{
		instances: instances,
		hashing:   hashing,
		buckets:   buckets,
	}
}
