package mr

type Queue struct {
	data []interface{}
}

func (q *Queue) Offer(k interface{}) {
	q.data = append(q.data, k)
}

func (q *Queue) Pop() interface{} {
	if len(q.data) == 0 {
		return nil
	}
	v := q.data[0]
	q.data = q.data[1:]
	return v
}
