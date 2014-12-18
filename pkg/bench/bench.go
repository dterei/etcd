package bench

import (
	"sync"
  "time"
)

type Bench interface {
	Start(id uint64) time.Time
  Snap(id uint64) time.Duration
	End(id uint64) time.Duration
}

type timing struct {
  start time.Time
  snap time.Time
}

type list struct {
	l sync.Mutex
	m map[uint64]timing
}

var defaultBench Bench = &list{m: make(map[uint64]timing)}

func Start(id uint64) time.Time {
  return defaultBench.Start(id)
}

func Snap(id uint64) time.Duration {
  return defaultBench.Snap(id)
}

func End(id uint64) time.Duration {
  return defaultBench.End(id)
}

func New() Bench {
	return &list{m: make(map[uint64]timing)}
}

func (b *list) Start(id uint64) time.Time {
	b.l.Lock()
	defer b.l.Unlock()
	t, ok := b.m[id]
	if !ok {
    now := time.Now()
    b.m[id] = timing{ now, now }
    return now
	} else {
    return t.start
  }
}

func (b *list) Snap(id uint64) (snap time.Duration) {
	b.l.Lock()
	defer b.l.Unlock()
	t, ok := b.m[id]
	if ok {
    now := time.Now()
    snap = now.Sub(t.snap)
    t.snap = now
    b.m[id] = t
    return snap
	} else {
    return 0
  }
}

func (b *list) End(id uint64) (snap time.Duration) {
  b.l.Lock()
  defer b.l.Unlock()
  t, ok := b.m[id]
  if ok {
    delete(b.m, id)
    return time.Since(t.start)
  } else {
    return 0
  }
}

