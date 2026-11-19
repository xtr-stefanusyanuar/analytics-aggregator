package handler

import (
	"analytics-aggregator/internal/adapter/repository"
	"analytics-aggregator/internal/adapter/repository/sqlc"
	"analytics-aggregator/internal/config"
	"analytics-aggregator/internal/core/service"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// stubEnricher replaces the external geo API so the test only depends on Postgres.
type stubEnricher struct{}

func (stubEnricher) EnrichIp(_ context.Context, _ string) ([]byte, error) {
	return []byte(`{"country":"test"}`), nil
}

// startPostgres runs a throwaway Postgres 16 container with the project migration applied.
func startPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("analytics_aggregator"),
		postgres.WithUsername("analytics"),
		postgres.WithPassword("analytics"),
		postgres.WithInitScripts(filepath.Join("..", "..", "..", "db", "migrations", "001_init.up.sql")),
		postgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// AC7: accepted events are persisted. N events posted through the real router,
// pipeline service and Postgres repository end up as N rows in the events table.
func TestIntegration_AcceptedEventsArePersisted(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in -short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t) // skip cleanly when Docker is not available

	// Arrange
	ctx := context.Background()
	pool := startPostgres(t)
	queries := sqlc.New(pool)

	cfg := &config.Config{
		EnricherWorkerSize: 2,
		InsertBatchSize:    5,
		PipelineJobSize:    50,
		BackoffMulitplier:  time.Millisecond,
	}
	pipelineCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	ps := service.NewPipelineService(pipelineCtx, repository.NewPostgresTxManager(pool), stubEnricher{}, cfg)
	ps.Open(pipelineCtx)
	router := NewRouter(ps, cfg)

	// 7 = one full batch of 5 (flushed on arrival) + 2 left over (flushed by the ticker),
	// so both flush paths are exercised.
	const n = 7
	before := testutil.ToFloat64(eventsAcceptedTotal)

	// Act
	for i := range n {
		body := fmt.Sprintf(`{"event_type":"page_view","user_id":"u-%d","client_ip":"203.0.113.%d"}`, i, i)
		req := httptest.NewRequest(http.MethodPost, "/event/", strings.NewReader(body))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusAccepted, rec.Code, "event %d: %s", i, rec.Body.String())
	}

	// Assert: writes are async (batch + 3s ticker), so poll the database.
	require.Eventually(t, func() bool {
		count, err := queries.CountEvents(ctx)
		return err == nil && count == n
	}, 15*time.Second, 200*time.Millisecond, "expected %d rows in events", n)

	ps.Close()

	count, err := queries.CountEvents(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, n, count, "no extra rows after shutdown flush")

	var deadLetters int64
	require.NoError(t, pool.QueryRow(ctx, "SELECT COUNT(*) FROM dead_letter_events").Scan(&deadLetters))
	assert.Zero(t, deadLetters, "no event should be dead-lettered on the happy path")

	assert.Equal(t, float64(n), testutil.ToFloat64(eventsAcceptedTotal)-before, "counter must match accepted events")
}
