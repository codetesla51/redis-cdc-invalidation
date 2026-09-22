package main

import (
	"fmt"
	"sync"
	"testing"
)

func TestRouterOwnerStable(t *testing.T) {
	r := NewRouter(8)
	defer r.StopAndWait()

	cases := []struct {
		name string
		id   string
	}{
		{"simple", "p1"},
		{"other", "p2"},
		{"prefixed", "product:123"},
		{"empty", ""},
		{"alpha", "abc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first := r.Owner(tc.id)
			if first < 0 || first >= 8 {
				t.Fatalf("Owner(%q) = %d, want in [0,8)", tc.id, first)
			}
			for i := 0; i < 10; i++ {
				if got := r.Owner(tc.id); got != first {
					t.Fatalf("Owner(%q) changed: %d vs %d", tc.id, first, got)
				}
			}
		})
	}
}

func TestNewRouterDefaultsToOne(t *testing.T) {
	for _, n := range []int{0, -3} {
		r := NewRouter(n)
		if got := r.Owner("anything"); got != 0 {
			t.Fatalf("NewRouter(%d).Owner = %d, want 0 (single pool)", n, got)
		}
		r.StopAndWait()
	}
}

func TestRouterSameKeyOrdered(t *testing.T) {
	r := NewRouter(8)
	defer r.StopAndWait()

	const tasks = 50
	var mu sync.Mutex
	var got []int
	var wg sync.WaitGroup
	for i := 0; i < tasks; i++ {
		wg.Add(1)
		i := i
		r.Dispatch("hot-key", func() {
			defer wg.Done()
			mu.Lock()
			got = append(got, i)
			mu.Unlock()
		})
	}
	wg.Wait()

	if len(got) != tasks {
		t.Fatalf("ran %d tasks, want %d", len(got), tasks)
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("out of order at %d: got %d, want %d", i, v, i)
		}
	}
}

func TestRouterDispatchRuns(t *testing.T) {
	r := NewRouter(4)
	defer r.StopAndWait()

	done := make(chan string, 3)
	for _, id := range []string{"p1", "p2", "p3"} {
		id := id
		r.Dispatch(id, func() { done <- id })
	}
	r.StopAndWait()
	close(done)

	seen := map[string]bool{}
	for id := range done {
		seen[id] = true
	}
	for _, id := range []string{"p1", "p2", "p3"} {
		if !seen[id] {
			t.Fatalf("task %q did not run", id)
		}
	}
}

func TestRouterSpreadsKeys(t *testing.T) {
	r := NewRouter(8)
	defer r.StopAndWait()

	used := map[int]bool{}
	for i := 0; i < 100; i++ {
		used[r.Owner(fmt.Sprintf("product-%d", i))] = true
	}
	if len(used) < 2 {
		t.Fatalf("100 keys used %d pool(s), want spread across >1", len(used))
	}
}
