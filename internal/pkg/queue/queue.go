package queue

import (
	"github.com/eapache/queue"
)

type Queue[T PolicyQueueHandle | TaskQueueHandle] struct {
	queue *queue.Queue
}

// PolicyQueue 存放待处理policy队列
var PolicyQueue = &Queue[PolicyQueueHandle]{
	queue: queue.New(),
}

var TaskQueue = &Queue[TaskQueueHandle]{
	queue: queue.New(),
}

type PolicyQueueHandle struct {
	ID       uint   `json:"id"`
	HandleID string `json:"handle_id"`
}

type TaskQueueHandle struct {
	ID       uint   `json:"id"`
	PolicyID uint   `json:"policy_id"`
	HandleID string `json:"handle_id"`
}

func (c *Queue[T]) Push(item T) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
		}
	}()

	equal := func(v1, v2 any) bool {
		switch o := v1.(type) {
		case PolicyQueueHandle:
			return o.ID == v2.(PolicyQueueHandle).ID
		case TaskQueueHandle:
			return o.ID == v2.(TaskQueueHandle).ID
		}
		return false
	}

	for i := 0; ; i++ {
		if i >= c.queue.Length() {
			break
		}

		elem := c.queue.Get(i)

		if equal(item, elem) {
			return false
		}
	}
	c.queue.Add(item)
	return true
}

func (c *Queue[T]) Pop() (T, bool) {
	var ret T
	if c.queue.Length() == 0 {
		return ret, false
	}
	elem := c.queue.Remove()
	if v, ok := elem.(T); ok {
		return v, true
	}
	return ret, false
}

func (c *Queue[T]) List() (ret []T) {
	if c.queue.Length() == 0 {
		return ret
	}

	defer func() {
		if r := recover(); r != nil {
			ret = []T{}
		}
	}()

	for i := 0; ; i++ {
		if i >= c.queue.Length() {
			break
		}

		elem := c.queue.Get(i)

		ret = append(ret, elem.(T))
	}
	return ret
}
