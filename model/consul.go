package model

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	log "github.com/golang/glog"
	consul "github.com/hashicorp/consul/api"
)

type Secrets struct {
	ConsulToken string
}

type raw struct {
	ConsulToken string `json:"consul_token"`
}

var (
	secretsLock  sync.Mutex
	secrets      = &Secrets{}
	ConsulTokenC = make(chan struct{})
)

func ReadEvery(fname string, tick time.Duration) error {
	err := readSecretsOnceFromFile(fname)
	ticker := time.NewTicker(tick)
	go func() {
		for range ticker.C {
			if err := readSecretsOnceFromFile(fname); err != nil {
				log.Errorf("unable to reload secrets from %s: %v", fname, err)
			}
		}
	}()
	return err
}

func InitForTests(json string) error {
	return parseSecrets([]byte(json))
}

func Get() *Secrets {
	secretsLock.Lock()
	defer secretsLock.Unlock()
	return secrets
}

func (r *raw) validate() (*Secrets, error) {
	s := &Secrets{}
	s.ConsulToken = r.ConsulToken
	return s, nil
}

func readSecretsOnceFromFile(fname string) error {
	b, err := ioutil.ReadFile(fname)
	if err != nil {
		return fmt.Errorf("read failed: %v", err)
	}
	return parseSecrets(b)
}

func parseSecrets(b []byte) error {
	var r raw
	if err := json.Unmarshal(b, &r); err != nil {
		return fmt.Errorf("json unmarshal failed: %v", err)
	}
	newSecrets, err := r.validate()
	if err != nil {
		return fmt.Errorf("secret validation failed: %v", err)
	}
	secretsLock.Lock()
	defer secretsLock.Unlock()
	if (secrets.ConsulToken) != "" && (newSecrets.ConsulToken != secrets.ConsulToken) {
		log.Info("Consul token got changed")
		ConsulTokenC <- struct{}{}
	}
	secrets = newSecrets

	return nil
}

func NewConsulClient() (*consul.Client, error) {
	config := consul.DefaultConfig()
	config.Token = Get().ConsulToken
	consulClient, err := consul.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("Consul error while creating a client %s", err.Error())
	}

	return consulClient, nil
}
