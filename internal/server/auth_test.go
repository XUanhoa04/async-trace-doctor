package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBearerAuthMiddleware(t *testing.T) {
	protected := BearerAuthMiddleware("secret")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	for _, tc := range []struct {
		name, path, auth string
		want             int
	}{
		{name: "missing", path: "/report", want: http.StatusUnauthorized},
		{name: "wrong", path: "/report", auth: "Bearer wrong", want: http.StatusUnauthorized},
		{name: "valid", path: "/report", auth: "Bearer secret", want: http.StatusNoContent},
		{name: "health bypass", path: "/health", want: http.StatusNoContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.Header.Set("Authorization", tc.auth)
			rr := httptest.NewRecorder()
			protected.ServeHTTP(rr, req)
			if rr.Code != tc.want {
				t.Fatalf("status = %d, want %d", rr.Code, tc.want)
			}
		})
	}
}

func TestBearerAuthDisabled(t *testing.T) {
	h := BearerAuthMiddleware("")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/report", nil))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("disabled auth status = %d", rr.Code)
	}
}
