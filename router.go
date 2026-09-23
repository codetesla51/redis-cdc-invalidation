package main

import (
	"hash/fnv"

	"github.com/alitto/pond/v2"
)

// Router dispatches row IDs to N pools with concurrency 1 each.
// Same ID always lands on the same pool (per-key order).
// Different IDs spread across pools (parallel across keys).
type Router struct {
	pools []pond.Pool
}

// NewRouter creates n pools, each with maxWorkers=1 and unbounded queue.
func NewRouter(n int) *Router {
	if n <= 0 {
		n = 1
	}
	r := &Router{pools: make([]pond.Pool, n)}
	for i := range r.pools {
		r.pools[i] = pond.NewPool(1)
	}
	return r
}

func (r *Router) Owner(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(len(r.pools)))
}

func (r *Router) Dispatch(id string, fn func()) {
	r.pools[r.Owner(id)].Submit(fn)
}

func (r *Router) StopAndWait() {
	for _, p := range r.pools {
		p.StopAndWait()
	}
}
