package rediscache 

import (
	"encoding/json"
	"time"

	"github.com/go-redis/redis"
)

var ErrCacheMiss = redis.Nil

type Cache interface {
	Get(id string, res interface{}) error
	Set(id string, res interface{}) error
	SetRaw(id string, data []byte) error
	Delete(id string) error
	Expire(id string, at time.Time) error
	Begin(max time.Duration) Cache
	End() error
}

func New(client redis.UniversalClient, prefix string) Cache {
	return &rootCache{client, prefix}
}

type rootCache struct {
	r  redis.UniversalClient
	pr string
}

func (c *rootCache) Get(id string, res interface{}) error {
	key := c.pr + id
	b, err := c.r.Get(key).Bytes()
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, res)
	if err != nil {
		return err
	}
	return nil
}

func (c *rootCache) Set(id string, res interface{}) error {
	b, err := json.Marshal(res)
	if err != nil {
		return err
	}
	return c.SetRaw(id, b)
}

func (c *rootCache) SetRaw(id string, data []byte) error {
	key := c.pr + id
	err := c.r.Set(key, data, time.Hour).Err()
	if err != nil {
		return err
	}
	return nil
}

func (c *rootCache) Expire(id string, at time.Time) error {
	key := c.pr + id
	err := c.r.ExpireAt(key, at).Err()
	if err == redis.Nil {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

func (c *rootCache) Delete(id string) error {
	key := c.pr + id
	err := c.r.Del(key).Err()
	if err != nil {
		return err
	}
	return nil
}

func (c *rootCache) Begin(max time.Duration) Cache {
	return &txnCache{
		expireAt: time.Now().Add(max),
		parent:   c,
		mutates:  make(map[string][]byte),
	}
}

func (c *rootCache) End() error {
	return nil
}

type txnCache struct {
	parent   Cache
	mutates  map[string][]byte
	expireAt time.Time
}

func (c *txnCache) Get(id string, res interface{}) error {
	v, ok := c.mutates[id]
	if ok {
		if v == nil {
			return ErrCacheMiss
		}
		err := json.Unmarshal(v, res)
		if err != nil {
			return err
		}
		return nil
	}
	return c.parent.Get(id, res)
}

func (c *txnCache) Set(id string, res interface{}) error {
	b, err := json.Marshal(res)
	if err != nil {
		return err
	}
	c.mutates[id] = b
	return c.parent.Expire(id, c.expireAt)
}

func (c *txnCache) Expire(id string, at time.Time) error {
	return c.parent.Expire(id, at)
}

func (c *txnCache) SetRaw(id string, data []byte) error {
	return c.parent.SetRaw(id, data)
}

func (c *txnCache) Delete(id string) error {
	c.mutates[id] = nil
	return c.parent.Expire(id, c.expireAt)
}

func (c *txnCache) Begin(max time.Duration) Cache {
	return nil
}

func (c *txnCache) End() error {
	for k, v := range c.mutates {
		if v == nil {
			err := c.parent.Delete(k)
			if err != nil {
				return err
			}
		} else {
			err := c.parent.SetRaw(k, v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

