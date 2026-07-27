package cache

import (
	"sync"
	"sync/atomic"
)

// cacheLifecycle owns operation admission and the shutdown barriers for work
// that may still use cache resources after an operation returns.
type cacheLifecycle struct {
	closeOnce sync.Once
	closeErr  error

	operationMu sync.Mutex
	operationWg sync.WaitGroup
	replicaWg   sync.WaitGroup
	closed      atomic.Bool
}

func (l *cacheLifecycle) beginOperation() bool {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	if l.closed.Load() {
		return false
	}
	l.operationWg.Add(1)
	return true
}

func (l *cacheLifecycle) finishOperation() {
	l.operationWg.Done()
}

func (l *cacheLifecycle) closeAdmission() {
	l.operationMu.Lock()
	l.closed.Store(true)
	l.operationMu.Unlock()
}

func (l *cacheLifecycle) isClosed() bool {
	return l.closed.Load()
}

func (l *cacheLifecycle) waitForOperations() {
	l.operationWg.Wait()
}

// beginReplication must be called by an admitted operation before that
// operation calls finishOperation. This makes the later replica wait safe from
// concurrent WaitGroup additions.
func (l *cacheLifecycle) beginReplication() {
	l.replicaWg.Add(1)
}

func (l *cacheLifecycle) finishReplication() {
	l.replicaWg.Done()
}

func (l *cacheLifecycle) waitForReplication() {
	l.replicaWg.Wait()
}

func (l *cacheLifecycle) recordCloseError(err error) {
	if err != nil && l.closeErr == nil {
		l.closeErr = err
	}
}
