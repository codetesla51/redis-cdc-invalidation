package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/codetesla51/phylax"
)

// Config carries startup settings. Zero values are useless here, so main
// builds it entirely from the environment (see README for variables).
type Config struct {
	DSN       string
	RedisAddr string
	HTTPAddr  string
	Tables    []string
	Pools     int
	ChangeBuf int // 0 = phylax default (100); each buffered change ≈ 1KB
}

func main() {
	cfg := Config{
		DSN:       os.Getenv("DATABASE_URL"),
		RedisAddr: envOr("REDIS_ADDR", "localhost:6379"),
		HTTPAddr:  envOr("HTTP_ADDR", ":8080"),
		Tables:    parseTables(os.Getenv("TABLES")),
		Pools:     positiveEnv("WORKER_POOLS", 8),
		ChangeBuf: positiveEnv("CHANGE_BUFFER_SIZE", 0),
	}
	if err := run(context.Background(), cfg); err != nil {
		log.Fatal(err)
	}
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// positiveEnv reads a positive-int variable or returns def when unset.
// Set-but-invalid is fatal: silently running with an unasked-for value is worse.
func positiveEnv(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Fatalf("%s must be a positive int, got %q", name, v)
	}
	return n
}

// parseTables splits TABLES; empty means just products.
func parseTables(v string) []string {
	var tables []string
	for _, t := range strings.Split(v, ",") {
		if t = strings.TrimSpace(t); t != "" {
			tables = append(tables, t)
		}
	}
	if len(tables) == 0 {
		tables = []string{"products"}
	}
	return tables
}

// run starts the phylax watcher plus its console, or no-ops when DSN is empty.
func run(ctx context.Context, cfg Config) error {
	router := NewRouter(cfg.Pools)
	defer router.StopAndWait()

	rdb := newRedisClient(cfg.RedisAddr)
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping %s: %w", cfg.RedisAddr, err)
	}

	if cfg.DSN == "" {
		fmt.Println("phylax DSN empty, skipping Start")
		return nil
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cdc, err := phylax.New(phylax.Config{
		DSN:              cfg.DSN,
		Tables:           cfg.Tables,
		ChangeBufferSize: cfg.ChangeBuf,
	})
	if err != nil {
		return err
	}

	cdc.OnChange(func(c *phylax.Change) {
		id, ok := rowID(c)
		if !ok {
			log.Printf("skip %s %s without row id", c.Operation, c.Table)
			return
		}
		router.Dispatch(id, func() {
			// No per-key logging here: at flood rates, formatting stdout per
			// DEL is the slowest stage and overflows the change buffer.
			// Flow is visible via the console (changes_processed) instead.
			if err := invalidateProduct(context.Background(), rdb, c.Table, id); err != nil {
				log.Printf("invalidate %s: %v", cacheKey(c.Table, id), err)
			}
		})
	})

	return cdcStream(ctx, cdc, cfg.HTTPAddr)
}

// cdcStream supervises replication and the console together: either one
// dying takes the other down, and Ctrl-C shuts both down gracefully.
func cdcStream(ctx context.Context, cdc *phylax.CDC, httpAddr string) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := cdc.Server()
	srvErr := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe(httpAddr)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		srvErr <- err
	}()
	fmt.Printf("console at http://localhost%s/dashboard\n", httpAddr)

	cdcErr := make(chan error, 1)
	go func() { cdcErr <- cdc.Start(ctx) }()

	shutdown := func() {
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
	}

	select {
	case err := <-srvErr:
		stop()
		<-cdcErr // Start returns nil once ctx is cancelled.
		return err
	case err := <-cdcErr:
		shutdown()
		<-srvErr
		return err
	case <-ctx.Done():
		shutdown()
		<-srvErr
		return <-cdcErr
	}
}

// rowID extracts the row id for routing. Deletes carry it in OldRow,
// other ops in NewRow. Values are strings (phylax decodes text tuples);
// anything else — TRUNCATE carries no rows at all — reports missing
// instead of guessing, and the caller skips it.
func rowID(c *phylax.Change) (string, bool) {
	rows := c.NewRow
	if c.Operation == "delete" {
		rows = c.OldRow
	}
	id, ok := rows["id"].(string)
	return id, ok && id != ""
}
