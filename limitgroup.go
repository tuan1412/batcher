package batcher

import (
	"sync"
)

var sentinel = struct{}{}

type LimitGroup struct {
	sync.WaitGroup
	slots chan struct{}
}

func NewLimitGroup(limit uint) *LimitGroup {
	if limit == 0 {
		panic("zero is not a valid limit")
	}
	slots := make(chan struct{}, limit)
	return &LimitGroup{slots: slots}
}

func (l *LimitGroup) Add(delta int) {
	if delta > cap(l.slots) {
		panic("delta greater than limit")
	}
	if delta == 0 {
		return
	}

	if delta > 0 {
		l.WaitGroup.Add(delta)
		for i := 0; i < delta; i++ {
			l.slots <- sentinel
		}
	} else {
		for i := 0; i > delta; i-- {
			select {
			case <-l.slots:
			default:
				panic("trying to return more slots than acquired")
			}
		}
		l.WaitGroup.Add(delta)
	}
}

func (l *LimitGroup) Done() {
	select {
	case <-l.slots:
	default:
		panic("trying to return more slots than acquired")
	}
	l.WaitGroup.Done()
}
