package mr

import "sync"

// my implementation of concurrent queue
type Queue struct {
	data []interface{}
	size int
	mu   sync.Mutex
}

func (q *Queue) Offer(k interface{}) {
	for {
		if q.mu.TryLock() {
			q.data = append(q.data, k)
			q.size++

			q.mu.Unlock()
			break
		}
	}
}

func (q *Queue) Pop() interface{} {

	var res interface{}
	for {
		if q.mu.TryLock() {
			if len(q.data) == 0 {
				return nil
			}
			res = q.data[0]
			q.data = q.data[1:]
			q.size--

			q.mu.Unlock()
			break
		}
	}
	return res
}
