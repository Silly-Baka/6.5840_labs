package mr

type Queue struct {
	data []interface{}
	size int
}

func (q *Queue) Offer(k interface{}) {
	q.data = append(q.data, k)
	q.size++
}

func (q *Queue) Pop() interface{} {
	if len(q.data) == 0 {
		return nil
	}
	v := q.data[0]
	q.data = q.data[1:]
	q.size--
	return v
}

const (
	MapTaskType    = 0
	ReduceTaskType = 1
)
