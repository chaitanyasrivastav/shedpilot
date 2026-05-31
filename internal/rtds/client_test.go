package rtds

import (
	"context"
	"testing"
)

func TestNewClient_DefaultAddress(t *testing.T) {
	c := NewClient("")
	if c.address != IstiodRTDSAddress {
		t.Errorf("address = %q, want %q", c.address, IstiodRTDSAddress)
	}
}

func TestNewClient_CustomAddress(t *testing.T) {
	c := NewClient("custom-istiod:15010")
	if c.address != "custom-istiod:15010" {
		t.Errorf("address = %q, want custom-istiod:15010", c.address)
	}
}

func TestConnected_FalseBeforeConnect(t *testing.T) {
	c := NewClient("")
	if c.Connected() {
		t.Error("Connected() should be false before Connect() is called")
	}
}

func TestPush_NotConnected_QueuesPending(t *testing.T) {
	c := NewClient("")
	// Not connected — push should queue
	err := c.Push(context.Background(), "test-layer", map[string]interface{}{
		"admission_control.sr_threshold": 85.0,
	})
	// Error expected (not connected)
	if err == nil {
		t.Error("expected error when not connected")
	}
	// Should be queued
	if c.PendingCount() != 1 {
		t.Errorf("PendingCount() = %d, want 1", c.PendingCount())
	}
}

func TestPush_MultipleLayersQueued(t *testing.T) {
	c := NewClient("")
	layers := []string{"layer-a", "layer-b", "layer-c"}
	for _, l := range layers {
		_ = c.Push(context.Background(), l, map[string]interface{}{"key": 1.0})
	}
	if c.PendingCount() != 3 {
		t.Errorf("PendingCount() = %d, want 3", c.PendingCount())
	}
}

func TestPendingCount_Zero_WhenEmpty(t *testing.T) {
	c := NewClient("")
	if c.PendingCount() != 0 {
		t.Errorf("PendingCount() = %d, want 0 on fresh client", c.PendingCount())
	}
}

func TestClose_NotConnected_NoError(t *testing.T) {
	c := NewClient("")
	if err := c.Close(); err != nil {
		t.Errorf("Close() on unconnected client returned error: %v", err)
	}
}
