package ingest

import "testing"

func FuzzDecodeJSON(f *testing.F) {
	f.Add([]byte(`{"resourceSpans":[]}`))
	f.Add([]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[]}]}]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeJSON(data, nil)
	})
}

func FuzzNormalizeIDs(f *testing.F) {
	f.Add([]byte(`{"traceId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeRequestJSON(data)
	})
}
