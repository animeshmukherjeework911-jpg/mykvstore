package pubsub

import "sync"

type PubSub struct {
	mu   sync.RWMutex
	subs map[string]map[uint64]chan string
	next uint64
}

func NewPubSub() *PubSub {
	return &PubSub{
		subs: make(map[string]map[uint64]chan string),
	}
}

func (ps *PubSub) Subscribe(channel string) (chan string, uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.subs[channel] == nil {
		ps.subs[channel] = make(map[uint64]chan string)
	}

	ps.next++
	id := ps.next

	ch := make(chan string, 64)
	ps.subs[channel][id] = ch
	return ch, id
}

func (ps *PubSub) Unsubscribe(channel string, id uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.subs[channel] == nil {
		return
	}

	if ch, ok := ps.subs[channel][id]; ok {
		close(ch)
		delete(ps.subs[channel], id)
	}

	if len(ps.subs[channel]) == 0 {
		delete(ps.subs, channel)
	}
}

func (ps *PubSub) Publish(channel, message string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	subscribers := ps.subs[channel]
	if len(subscribers) == 0 {
		return 0
	}

	count := 0
	for _, ch := range subscribers {
		select {
		case ch <- message:
			count++
		default:

		}
	}
	return count
}

func (ps *PubSub) NumSubscribers(channel string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return len(ps.subs[channel])
}
