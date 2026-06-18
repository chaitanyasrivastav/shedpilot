package rtds

import (
	"context"
	"testing"
)

func TestConnected_FalseBeforeConnect(t *testing.T) {
	c, err := NewClient(nil, nil)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if c.Connected() {
		t.Error("Connected() should be false before Connect() is called")
	}
}

func TestPush_NotConnected_QueuesPending(t *testing.T) {
	c, err := NewClient(nil, nil)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	// Not connected — push should return error
	err = c.Push(context.Background(), "test-layer", map[string]interface{}{
		"admission_control.sr_threshold": 85.0,
	})
	if err == nil {
		t.Error("expected error when not connected")
	}
	// When disabled, nothing is queued
	if c.PendingCount() != 0 {
		t.Errorf("PendingCount() = %d, want 0 for disabled client", c.PendingCount())
	}
}

func TestPush_MultipleLayersQueued(t *testing.T) {
	c, err := NewClient(nil, nil)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	layers := []string{"layer-a", "layer-b", "layer-c"}
	for _, l := range layers {
		_ = c.Push(context.Background(), l, map[string]interface{}{"key": 1.0})
	}
	// When disabled, nothing is queued
	if c.PendingCount() != 0 {
		t.Errorf("PendingCount() = %d, want 0 for disabled client", c.PendingCount())
	}
}

func TestPendingCount_Zero_WhenEmpty(t *testing.T) {
	c, err := NewClient(nil, nil)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if c.PendingCount() != 0 {
		t.Errorf("PendingCount() = %d, want 0 on fresh client", c.PendingCount())
	}
}

func TestClose_NotConnected_NoError(t *testing.T) {
	c, err := NewClient(nil, nil)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close() on unconnected client returned error: %v", err)
	}
}
