package pubsub

import (
	"testing"
	"time"
)

func TestSubscribeReceivesPublish(t *testing.T) {
	ps := NewPubSub()
	ch, id := ps.Subscribe("news")
	defer ps.Unsubscribe("news", id)

	ps.Publish("news", "hello")

	select {
	case msg := <-ch:
		if msg != "hello" {
			t.Fatalf("want hello, got %q", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestPublishReturnsSubscriberCount(t *testing.T) {
	ps := NewPubSub()
	_, id1 := ps.Subscribe("ch")
	_, id2 := ps.Subscribe("ch")
	defer ps.Unsubscribe("ch", id1)
	defer ps.Unsubscribe("ch", id2)

	n := ps.Publish("ch", "msg")
	if n != 2 {
		t.Fatalf("want 2 receivers, got %d", n)
	}
}

func TestPublishNoSubscribersReturnsZero(t *testing.T) {
	ps := NewPubSub()
	if n := ps.Publish("empty", "msg"); n != 0 {
		t.Fatalf("want 0, got %d", n)
	}
}

func TestUnsubscribeClosesChannel(t *testing.T) {
	ps := NewPubSub()
	ch, id := ps.Subscribe("ch")
	ps.Unsubscribe("ch", id)

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel should be closed after Unsubscribe")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel close")
	}
}

func TestUnsubscribeRemovesFromPublish(t *testing.T) {
	ps := NewPubSub()
	_, id := ps.Subscribe("ch")
	ps.Unsubscribe("ch", id)

	if n := ps.Publish("ch", "msg"); n != 0 {
		t.Fatalf("want 0 after unsubscribe, got %d", n)
	}
}

func TestMultipleChannelsIndependent(t *testing.T) {
	ps := NewPubSub()
	ch1, id1 := ps.Subscribe("ch1")
	ch2, id2 := ps.Subscribe("ch2")
	defer ps.Unsubscribe("ch1", id1)
	defer ps.Unsubscribe("ch2", id2)

	ps.Publish("ch1", "only-ch1")

	select {
	case msg := <-ch1:
		if msg != "only-ch1" {
			t.Fatalf("ch1: want only-ch1, got %q", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out on ch1")
	}

	// ch2 should have nothing.
	select {
	case msg := <-ch2:
		t.Fatalf("ch2 unexpectedly received %q", msg)
	default:
	}
}

func TestNumSubscribers(t *testing.T) {
	ps := NewPubSub()
	if ps.NumSubscribers("ch") != 0 {
		t.Fatal("want 0 before any subscription")
	}
	_, id := ps.Subscribe("ch")
	if ps.NumSubscribers("ch") != 1 {
		t.Fatal("want 1 after subscribe")
	}
	ps.Unsubscribe("ch", id)
	if ps.NumSubscribers("ch") != 0 {
		t.Fatal("want 0 after unsubscribe")
	}
}
