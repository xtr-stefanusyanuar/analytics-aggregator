package handler

import (
	"analytics-aggregator/internal/core/domain"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeProcessor is a handwritten EventProcessor fake.
type fakeProcessor struct {
	affected int64
	err      error
	calls    int
}

func (f *fakeProcessor) ProcessAndStore(_ context.Context, _ *domain.Event) (int64, error) {
	f.calls++
	return f.affected, f.err
}

// captureLogs redirects the default slog logger to a JSON buffer for the
// duration of the test. slog's default logger is global, so tests using this
// must not call t.Parallel().
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// logLinesWithMsg returns every JSON log record whose "msg" equals msg.
func logLinesWithMsg(t *testing.T, buf *bytes.Buffer, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	for sc.Scan() {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &rec), "log line is not valid JSON: %s", sc.Text())
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

func newTestRouter(p EventProcessor) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	h := NewHandler(p, nil)
	r.Post("/event", h.MakeHandler(h.ProcessAndStoreEvents))
	return r
}

func TestHandler_ProcessAndStoreEvents(t *testing.T) {
	tests := []struct {
		name          string
		body          string
		processor     *fakeProcessor
		wantStatus    int
		wantCalls     int
		wantAccepted  bool     // expect counter +1 and one "event accepted" log line
		wantEventType string   // expected event_type field on the accepted log line
		forbiddenLogs []string // values that must never appear in any log output
	}{
		{
			name:          "AC1 happy path - 202, counter +1, one structured log line",
			body:          `{"event_type":"page_view"}`,
			processor:     &fakeProcessor{affected: 1},
			wantStatus:    http.StatusAccepted,
			wantCalls:     1,
			wantAccepted:  true,
			wantEventType: "page_view",
		},
		{
			name:       "AC2 malformed JSON - 400, no counter, no log",
			body:       `{"event_type":`,
			processor:  &fakeProcessor{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "AC2 unknown field - 400, no counter, no log",
			body:       `{"event_type":"page_view","unexpected":1}`,
			processor:  &fakeProcessor{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "AC3 missing event_type - 400, no counter, no log",
			body:       `{"user_id":"u-1"}`,
			processor:  &fakeProcessor{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "AC3 empty event_type - 400, no counter, no log",
			body:       `{"event_type":""}`,
			processor:  &fakeProcessor{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "AC4 pipeline failure - 500, no counter, no log",
			body:       `{"event_type":"page_view"}`,
			processor:  &fakeProcessor{err: errors.New("queue full")},
			wantStatus: http.StatusInternalServerError,
			wantCalls:  1,
		},
		{
			name:          "AC5 no PII - user_id, client_ip and payload are not logged",
			body:          `{"event_type":"checkout","user_id":"user-secret-123","client_ip":"203.0.113.77","payload":{"card":"4111-1111-1111-1111"}}`,
			processor:     &fakeProcessor{affected: 1},
			wantStatus:    http.StatusAccepted,
			wantCalls:     1,
			wantAccepted:  true,
			wantEventType: "checkout",
			forbiddenLogs: []string{"user-secret-123", "203.0.113.77", "4111-1111-1111-1111"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			logs := captureLogs(t)
			before := testutil.ToFloat64(eventsAcceptedTotal)
			router := newTestRouter(tt.processor)
			req := httptest.NewRequest(http.MethodPost, "/event", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()

			// Act
			router.ServeHTTP(rec, req)

			// Assert
			require.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
			assert.Equal(t, tt.wantCalls, tt.processor.calls, "ProcessAndStore call count")

			delta := testutil.ToFloat64(eventsAcceptedTotal) - before
			accepted := logLinesWithMsg(t, logs, "event accepted")

			if !tt.wantAccepted {
				assert.Zero(t, delta, "counter must not change on rejected requests")
				assert.Empty(t, accepted, "no 'event accepted' log on rejected requests")
				return
			}

			assert.Equal(t, float64(1), delta, "counter must increase by exactly 1")
			require.Len(t, accepted, 1, "exactly one 'event accepted' log line")

			var resp ProcessEventResponse
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			assert.Equal(t, "accepted", resp.Status)
			require.NotEmpty(t, resp.Id)

			line := accepted[0]
			assert.Equal(t, "INFO", line["level"])
			assert.Equal(t, resp.Id, line["event_id"], "log event_id must match response id")
			assert.Equal(t, tt.wantEventType, line["event_type"])
			assert.NotEmpty(t, line["request_id"], "request_id must be populated")

			for _, v := range tt.forbiddenLogs {
				assert.NotContains(t, logs.String(), v, "PII leaked into logs")
			}
		})
	}
}

// AC6: the Prometheus scrape endpoint is wired and exposes the new counter.
func TestRouter_MetricsEndpoint(t *testing.T) {
	router := NewRouter(nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "analytics_ingest_events_accepted_total")
}
