package server

import (
	"testing"
	"time"

	"github.com/yabby-groups/tunnel/internal/protocol"
)

func TestWebSocketEventQueuePreservesBurstOrder(t *testing.T) {
	queue := newWebSocketEventQueue()
	defer queue.Close()

	const count = 4096
	for index := range count {
		queue.Push(protocol.Message{ID: "socket", StatusCode: index})
	}
	for index := range count {
		select {
		case message := <-queue.Output():
			if message.StatusCode != index {
				t.Fatalf("message %d has status %d", index, message.StatusCode)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for message %d", index)
		}
	}
}
