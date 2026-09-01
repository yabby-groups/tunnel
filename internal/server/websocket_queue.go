package server

import (
	"sync"

	"github.com/yabby-groups/tunnel/internal/protocol"
)

// webSocketEventQueue preserves one WebSocket stream's ordering without
// allowing a slow browser to block the shared tunnel control connection.
type webSocketEventQueue struct {
	mu      sync.Mutex
	pending []protocol.Message
	notify  chan struct{}
	output  chan protocol.Message
	closed  chan struct{}
	once    sync.Once
}

func newWebSocketEventQueue() *webSocketEventQueue {
	queue := &webSocketEventQueue{
		notify: make(chan struct{}, 1),
		output: make(chan protocol.Message),
		closed: make(chan struct{}),
	}
	go queue.run()
	return queue
}

func (q *webSocketEventQueue) Push(message protocol.Message) {
	q.mu.Lock()
	select {
	case <-q.closed:
		q.mu.Unlock()
		return
	default:
		q.pending = append(q.pending, message)
	}
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *webSocketEventQueue) Output() <-chan protocol.Message {
	return q.output
}

func (q *webSocketEventQueue) Close() {
	q.once.Do(func() { close(q.closed) })
}

func (q *webSocketEventQueue) takePending() []protocol.Message {
	q.mu.Lock()
	deferred := q.pending
	q.pending = nil
	q.mu.Unlock()
	return deferred
}

func (q *webSocketEventQueue) run() {
	defer close(q.output)
	queued := make([]protocol.Message, 0)
	for {
		if len(queued) == 0 {
			select {
			case <-q.notify:
				queued = append(queued, q.takePending()...)
			case <-q.closed:
				return
			}
			continue
		}
		select {
		case q.output <- queued[0]:
			queued = queued[1:]
		case <-q.notify:
			queued = append(queued, q.takePending()...)
		case <-q.closed:
			return
		}
	}
}
