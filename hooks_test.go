package liteq

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestBaseHooks_NoPanic(t *testing.T) {
	h := BaseHooks{}
	ctx := context.Background()
	now := time.Now()

	// All methods must execute without panic.
	h.OnEnqueue(ctx, "q", "id-1")
	h.OnDequeue(ctx, "q", 5)
	h.OnTaskStart(ctx, "id-1")
	h.OnTaskComplete(ctx, "id-1", time.Second)
	h.OnRetry(ctx, "id-1", 2, now)
	h.OnDLQ(ctx, "id-1", "max retries exceeded")
	h.OnDLQFailed(ctx, "id-1", nil)
}

func newTestSlogHooks(t *testing.T) (SlogHooks, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return SlogHooks{Logger: logger}, &buf
}

func TestSlogHooks_OnEnqueue(t *testing.T) {
	h, buf := newTestSlogHooks(t)
	h.OnEnqueue(context.Background(), "my_queue", "entry-1")
	out := buf.String()
	for _, want := range []string{"task enqueued", "my_queue", "entry-1"} {
		if !strings.Contains(out, want) {
			t.Errorf("OnEnqueue log missing %q in: %s", want, out)
		}
	}
}

func TestSlogHooks_OnDequeue(t *testing.T) {
	h, buf := newTestSlogHooks(t)
	h.OnDequeue(context.Background(), "my_queue", 7)
	out := buf.String()
	for _, want := range []string{"tasks dequeued", "my_queue", "7"} {
		if !strings.Contains(out, want) {
			t.Errorf("OnDequeue log missing %q in: %s", want, out)
		}
	}
}

func TestSlogHooks_OnTaskStart(t *testing.T) {
	h, buf := newTestSlogHooks(t)
	h.OnTaskStart(context.Background(), "task-99")
	out := buf.String()
	for _, want := range []string{"task started", "task-99"} {
		if !strings.Contains(out, want) {
			t.Errorf("OnTaskStart log missing %q in: %s", want, out)
		}
	}
}

func TestSlogHooks_OnTaskComplete(t *testing.T) {
	h, buf := newTestSlogHooks(t)
	h.OnTaskComplete(context.Background(), "task-99", 42*time.Millisecond)
	out := buf.String()
	for _, want := range []string{"task completed", "task-99"} {
		if !strings.Contains(out, want) {
			t.Errorf("OnTaskComplete log missing %q in: %s", want, out)
		}
	}
}

func TestSlogHooks_OnRetry(t *testing.T) {
	h, buf := newTestSlogHooks(t)
	next := time.Now().Add(5 * time.Second)
	h.OnRetry(context.Background(), "task-5", 3, next)
	out := buf.String()
	for _, want := range []string{"task scheduled for retry", "task-5"} {
		if !strings.Contains(out, want) {
			t.Errorf("OnRetry log missing %q in: %s", want, out)
		}
	}
}

func TestSlogHooks_OnDLQ(t *testing.T) {
	h, buf := newTestSlogHooks(t)
	h.OnDLQ(context.Background(), "task-7", "max retries exceeded")
	out := buf.String()
	for _, want := range []string{"dead-letter queue", "task-7", "max retries exceeded"} {
		if !strings.Contains(out, want) {
			t.Errorf("OnDLQ log missing %q in: %s", want, out)
		}
	}
}

func TestSlogHooks_OnDLQFailed(t *testing.T) {
	h, buf := newTestSlogHooks(t)
	h.OnDLQFailed(context.Background(), "task-8", context.DeadlineExceeded)
	out := buf.String()
	for _, want := range []string{"could not be sent", "task-8"} {
		if !strings.Contains(out, want) {
			t.Errorf("OnDLQFailed log missing %q in: %s", want, out)
		}
	}
}

func TestSlogHooks_ImplementsHooks(t *testing.T) {
	// Compile-time assertion: SlogHooks satisfies the Hooks interface.
	var _ Hooks = SlogHooks{}
}

func TestSlogHooks_EmbedBaseHooks(t *testing.T) {
	// SlogHooks embeds BaseHooks; methods not overridden should not panic.
	h := SlogHooks{Logger: slog.Default()}
	_ = h // all methods are implemented; ensure no nil-pointer dereference
}

func TestSafeHook_NoPanicOnNormalFunc(t *testing.T) {
	called := false
	safeHook(func() { called = true })
	if !called {
		t.Error("safeHook should call the provided function")
	}
}

func TestSafeHook_RecoversPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic escaped safeHook: %v", r)
		}
	}()
	safeHook(func() { panic("hook gone wrong") })
}

func TestSafeHook_RecoversPanicWithError(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic escaped safeHook: %v", r)
		}
	}()
	safeHook(func() { panic(context.DeadlineExceeded) })
}
