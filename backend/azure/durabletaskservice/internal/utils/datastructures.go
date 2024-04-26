package utils

import (
	"container/list"
	"log"
	"sync"

	dtmbprotos "github.com/microsoft/durabletask-go/backend/azure/durabletaskservice/internal/backend/v1"
)

const (
	DefaultOrchestrationHistoryCacheSize int = 100
)

type SyncQueue[T dtmbprotos.ExecuteOrchestrationMessage | dtmbprotos.ExecuteActivityMessage | dtmbprotos.Event] struct {
	lock  *sync.RWMutex
	items []*T
}

func NewSyncQueue[T dtmbprotos.ExecuteOrchestrationMessage | dtmbprotos.ExecuteActivityMessage | dtmbprotos.Event]() SyncQueue[T] {
	return SyncQueue[T]{
		lock:  &sync.RWMutex{},
		items: []*T{},
	}
}

func (q *SyncQueue[T]) Enqueue(item ...*T) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.items = append(q.items, item...)
}

func (q *SyncQueue[T]) Dequeue() *T {
	q.lock.Lock()
	defer q.lock.Unlock()
	if len(q.items) == 0 {
		return nil
	}

	item := q.items[0]
	q.items[0] = nil // avoid a leak
	q.items = q.items[1:]
	return item
}

func (q *SyncQueue[T]) PeekAll() []*T {
	q.lock.RLock()
	defer q.lock.RUnlock()

	// create a manual copy of the items, not just a copy of the pointers
	ret := make([]*T, len(q.items))
	for i, p := range q.items {
		if p == nil {
			continue
		}
		v := *p //nolint:copylocks
		ret[i] = &v
	}

	return ret
}

func (q *SyncQueue[T]) PeekLast() *T {
	q.lock.RLock()
	defer q.lock.RUnlock()
	if len(q.items) == 0 {
		return nil
	}

	// return a copy of the last item
	if q.items[len(q.items)-1] == nil {
		return nil
	}
	var itemCopy T = *q.items[len(q.items)-1] //nolint:copylocks
	return &itemCopy
}

type OrchestrationTaskCounter struct {
	lock                    *sync.Mutex
	orchestrationCounterMap map[string]int32
	orchestrationTaskIDMap  map[string]map[int64]int32
}

func NewOrchestrationTaskIDManager() OrchestrationTaskCounter {
	return OrchestrationTaskCounter{
		lock:                    &sync.Mutex{},
		orchestrationCounterMap: make(map[string]int32),
		orchestrationTaskIDMap:  make(map[string]map[int64]int32),
	}
}

func (o *OrchestrationTaskCounter) GetTaskNumber(orchestrationID string, sequenceNumber int64) int32 {
	o.lock.Lock()
	defer o.lock.Unlock()

	if _, ok := o.orchestrationCounterMap[orchestrationID]; !ok {
		o.orchestrationCounterMap[orchestrationID] = 0
		o.orchestrationTaskIDMap[orchestrationID] = make(map[int64]int32)
	} else {
		if _, ok := o.orchestrationTaskIDMap[orchestrationID][sequenceNumber]; ok {
			return o.orchestrationTaskIDMap[orchestrationID][sequenceNumber]
		}
	}

	// we assign the previous counter value for the sequence ID and orchestration ID combination
	o.orchestrationTaskIDMap[orchestrationID][sequenceNumber] = o.orchestrationCounterMap[orchestrationID]
	// increment the counter for the next time
	o.orchestrationCounterMap[orchestrationID]++

	return o.orchestrationTaskIDMap[orchestrationID][sequenceNumber]
}

func (o *OrchestrationTaskCounter) PurgeOrchestration(orchestrationID string) {
	o.lock.Lock()
	defer o.lock.Unlock()

	if _, ok := o.orchestrationTaskIDMap[orchestrationID]; ok {
		delete(o.orchestrationTaskIDMap, orchestrationID)
		delete(o.orchestrationCounterMap, orchestrationID)
	}
}

type OrchestrationHistoryCache struct {
	lock *sync.Mutex

	capacity int
	cache    map[string]*list.Element
	list     *list.List
}

type orchestrationHistoryItem struct {
	orchestrationID string
	historyEvents   *SyncQueue[dtmbprotos.Event]
}

func NewOrchestrationHistoryCache(capacity *int) OrchestrationHistoryCache {
	if capacity == nil {
		return OrchestrationHistoryCache{
			lock:     &sync.Mutex{},
			capacity: DefaultOrchestrationHistoryCacheSize,
			cache:    make(map[string]*list.Element),
			list:     list.New(),
		}
	}

	return OrchestrationHistoryCache{
		capacity: *capacity,
		cache:    make(map[string]*list.Element),
		list:     list.New(),
	}
}

func (o *OrchestrationHistoryCache) GetCachedHistoryEventsForOrchestrationID(orchestrationID string) []*dtmbprotos.Event {
	o.lock.Lock()
	defer o.lock.Unlock()
	if element, ok := o.cache[orchestrationID]; ok {
		o.list.MoveToFront(element)
		return element.Value.(*orchestrationHistoryItem).historyEvents.PeekAll()
	}
	return nil
}

func (o *OrchestrationHistoryCache) AddHistoryEventsForOrchestrationID(orchestrationID string, events []*dtmbprotos.Event) {
	o.lock.Lock()
	defer o.lock.Unlock()

	if element, ok := o.cache[orchestrationID]; ok {
		o.list.MoveToFront(element)

		if element.Value.(*orchestrationHistoryItem).historyEvents == nil {
			queue := NewSyncQueue[dtmbprotos.Event]()
			element.Value.(*orchestrationHistoryItem).historyEvents = &queue
		}
		element.Value.(*orchestrationHistoryItem).historyEvents.Enqueue(events...)

		return
	}

	// Since we are adding a new Orchestration History, we need to check if we need to evict the oldest one

	if o.list.Len() >= o.capacity {
		delete(o.cache, o.list.Back().Value.(*orchestrationHistoryItem).orchestrationID)
		o.list.Remove(o.list.Back())
	}

	queue := NewSyncQueue[dtmbprotos.Event]()
	queue.Enqueue(events...)
	orchestrationHistoryItem := orchestrationHistoryItem{
		orchestrationID: orchestrationID,
		historyEvents:   &queue,
	}

	o.cache[orchestrationID] = o.list.PushFront(
		&orchestrationHistoryItem)
}

func (o *OrchestrationHistoryCache) EvictCacheForOrchestrationID(orchestrationID string) {
	log.Println("=== Evicting cache for orchestrationID: ", orchestrationID)
	o.lock.Lock()
	defer o.lock.Unlock()
	if element, ok := o.cache[orchestrationID]; ok {
		delete(o.cache, orchestrationID)
		o.list.Remove(element)
	}
}

func LookUpTaskID(orchestrationId string, sequenceNumber int) int {
	return 0
}
