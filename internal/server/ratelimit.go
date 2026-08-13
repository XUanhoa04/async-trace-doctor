package server

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type tokenBucket struct {
	mu       sync.Mutex
	rate     float64
	burst    float64
	tokens   float64
	lastFill time.Time
}

func newTokenBucket(requestsPerSecond float64, burst int) *tokenBucket {
	if requestsPerSecond <= 0 || burst <= 0 {
		return nil
	}
	now := time.Now()
	return &tokenBucket{rate: requestsPerSecond, burst: float64(burst), tokens: float64(burst), lastFill: now}
}

func (b *tokenBucket) allow() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens += now.Sub(b.lastFill).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.lastFill = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func rateLimitMiddleware(bucket *tokenBucket, metrics *Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !bucket.allow() {
				metrics.RateLimited.WithLabelValues("http").Inc()
				w.Header().Set("Retry-After", strconv.Itoa(1))
				writeErrorJSON(w, http.StatusTooManyRequests, "ingest rate limit exceeded")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func rateLimitInterceptor(bucket *tokenBucket, metrics *Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !bucket.allow() {
			metrics.RateLimited.WithLabelValues("grpc").Inc()
			return nil, status.Error(codes.ResourceExhausted, "ingest rate limit exceeded")
		}
		return handler(ctx, req)
	}
}
