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
	"time"

	"github.com/codetesla51/phylax"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":8080"
	}
	tables := parseTables(os.Getenv("TABLES"))
	pools := 8
	if v := os.Getenv("WORKER_POOLS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			log.Fatalf("WORKER_POOLS must be a positive int, got %q", v)
		}
		pools = n
	}
	changeBuf := 0 // 0 = phylax default (100); each buffered change ≈ 1KB
	if v := os.Getenv("CHANGE_BUFFER_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			log.Fatalf("CHANGE_BUFFER_SIZE must be a positive int, got %q", v)
		}
		changeBuf = n
	}

	if err := run(context.Background(), dsn, redisAddr, httpAddr, tables, pools, changeBuf); err != nil {
		log.Fatal(err)
	}
}

// parseTables splits TABLES ("products,orders") into a table list.
// Empty means the default: just products.
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
func run(ctx context.Context, dsn, redisAddr, httpAddr string, tables []string, pools, changeBuf int) error {
	router := NewRouter(pools)
	defer router.StopAndWait()

	rdb := newRedisClient(redisAddr)
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping %s: %w", redisAddr, err)
	}

	if dsn == "" {
		fmt.Println("phylax DSN empty, skipping Start")
		return nil
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	cdc, err := phylax.New(phylax.Config{
		DSN:              dsn,
		Tables:           tables,
		ChangeBufferSize: changeBuf,
	})
	if err != nil {
		return err
	}

	cdc.OnChange(func(c *phylax.Change) {
		id := rowID(c)
		router.Dispatch(id, func() {
			// No per-key logging here: at flood rates, formatting stdout per
			// DEL is the slowest stage and overflows the change buffer.
			// Flow is visible via the console (changes_processed) instead.
			if err := invalidateProduct(context.Background(), rdb, c.Table, id); err != nil {
				log.Printf("invalidate %s: %v", cacheKey(c.Table, id), err)
			}
		})
	})

	return cdcStream(ctx, cdc, httpAddr)
}

// cdcStream supervises replication and the console together: either one
// dying takes the other down, and Ctrl-C shuts both down gracefully.
func cdcStream(ctx context.Context, cdc *phylax.CDC, httpAddr string) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
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
		return <-cdcErr // nil on a clean Ctrl-C.
	}
}

// rowID extracts the product id for routing.
// Deletes carry it in OldRow, other ops in NewRow.
func rowID(c *phylax.Change) string {
	if c.Operation == "delete" {
		if v, ok := c.OldRow["id"]; ok {
			return stringify(v)
		}
		return ""
	}
	if v, ok := c.NewRow["id"]; ok {
		return stringify(v)
	}
	return ""
}

func stringify(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}
