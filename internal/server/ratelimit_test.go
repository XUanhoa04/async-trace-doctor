package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTokenBucketCreation(t *testing.T) {
	if tb := newTokenBucket(0, 10); tb != nil {
		t.Errorf("expected nil token bucket for rate <= 0, got %#v", tb)
	}
	if tb := newTokenBucket(10, 0); tb != nil {
		t.Errorf("expected nil token bucket for burst <= 0, got %#v", tb)
	}
	if tb := newTokenBucket(-5, -5); tb != nil {
		t.Errorf("expected nil token bucket for negative inputs, got %#v", tb)
	}

	tb := newTokenBucket(100, 5)
	if tb == nil {
		t.Fatal("expected non-nil token bucket")
	}
	if tb.burst != 5 || tb.rate != 100 {
		t.Errorf("unexpected bucket parameters: burst=%f, rate=%f", tb.burst, tb.rate)
	}
}

func TestTokenBucketNilSafe(t *testing.T) {
	var tb *tokenBucket
	if !tb.allow() {
		t.Errorf("nil token bucket should always allow")
	}
}

func TestTokenBucketAllowAndDrain(t *testing.T) {
	tb := newTokenBucket(10, 3)
	for i := 0; i < 3; i++ {
		if !tb.allow() {
			t.Fatalf("expected token %d to be allowed", i)
		}
	}
	if tb.allow() {
		t.Fatalf("expected 4th token to be rejected because burst is 3")
	}
}

func TestTokenBucketRefill(t *testing.T) {
	tb := newTokenBucket(100, 2)
	if !tb.allow() || !tb.allow() {
		t.Fatal("expected first 2 tokens allowed")
	}
	if tb.allow() {
		t.Fatal("expected burst exhausted")
	}

	// Sleep enough time to regenerate at least 2 tokens (at 100/sec, 30ms = 3 tokens > burst of 2)
	time.Sleep(35 * time.Millisecond)

	if !tb.allow() {
		t.Fatal("expected token to be available after refill")
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	tb := newTokenBucket(1, 1)

	called := 0
	handler := rateLimitMiddleware(tb, m)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		w.WriteHeader(http.StatusOK)
	}))

	// First request succeeds
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/v1/traces", nil)
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK || called != 1 {
		t.Fatalf("first request failed: code=%d, called=%d", rec1.Code, called)
	}

	// Second request immediately after is rate limited
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/v1/traces", nil)
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", rec2.Code)
	}
	if retryAfter := rec2.Header().Get("Retry-After"); retryAfter != "1" {
		t.Errorf("expected Retry-After=1, got %q", retryAfter)
	}
	if called != 1 {
		t.Errorf("handler was called on rate limited request: called=%d", called)
	}
}

func TestRateLimitInterceptor(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	tb := newTokenBucket(1, 1)

	interceptor := rateLimitInterceptor(tb, m)
	dummyHandler := func(ctx context.Context, req any) (any, error) {
		return "success", nil
	}

	// First call succeeds
	res, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, dummyHandler)
	if err != nil || res != "success" {
		t.Fatalf("first call failed: %v, res=%v", err, res)
	}

	// Second call returns ResourceExhausted
	_, err = interceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, dummyHandler)
	if err == nil {
		t.Fatal("expected rate limit error on second call")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted error, got %v", err)
	}
}

func TestExportResponse(t *testing.T) {
	noRejection := exportResponse(AddResult{Accepted: 5, Rejected: 0})
	if noRejection.PartialSuccess != nil {
		t.Errorf("expected nil PartialSuccess for 0 rejections, got %#v", noRejection.PartialSuccess)
	}

	capacityRejection := exportResponse(AddResult{Accepted: 2, Rejected: 3})
	if capacityRejection.PartialSuccess == nil || capacityRejection.PartialSuccess.RejectedSpans != 3 {
		t.Fatalf("expected 3 rejected spans in PartialSuccess, got %#v", capacityRejection.PartialSuccess)
	}

	memoryRejection := exportResponse(AddResult{Accepted: 1, Rejected: 2, RejectedMemory: 2})
	if memoryRejection.PartialSuccess == nil || memoryRejection.PartialSuccess.RejectedSpans != 2 {
		t.Fatalf("expected 2 rejected spans for memory limit, got %#v", memoryRejection.PartialSuccess)
	}
}
