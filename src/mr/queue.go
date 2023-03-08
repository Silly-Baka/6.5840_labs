package mr

import (
	"fmt"
	"sync"
)

// my implementation of concurrent queue
type Queue struct {
	data []interface{}
	size int
	mu   sync.Mutex
}

func (q *Queue) Offer(k interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.data = append(q.data, k)
	q.size++
}

func (q *Queue) Pop() interface{} {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.data) == 0 {
		return nil
	}
	res := q.data[0]
	q.data = q.data[1:]
	q.size--

	return res
}

// toString()

func (q *Queue) String() string {
	return fmt.Sprintln(q.data)
}
