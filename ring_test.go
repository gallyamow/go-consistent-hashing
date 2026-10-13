package consistenthashing

import (
	"fmt"
	"sync"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
)

type StrHasher struct{}

func (h StrHasher) Hash(key string) uint64 {
	return xxhash.Sum64String(key)
}

var strHasher = StrHasher{}

func TestAddNode(t *testing.T) {
	t.Run("should add node", func(t *testing.T) {
		r := NewRing[string](strHasher)

		err := r.AddNode("a", 10)
		assert.NoError(t, err)
	})

	t.Run("should reject duplicate", func(t *testing.T) {
		r := NewRing[string](strHasher)

		err := r.AddNode("a", 10)
		assert.NoError(t, err)
		err = r.AddNode("a", 10)
		assert.ErrorIs(t, err, ErrNodeAlreadyExists)
	})

	t.Run("should reject non-positive weight", func(t *testing.T) {
		r := NewRing[string](strHasher)

		err := r.AddNode("a", 0)
		assert.ErrorIs(t, err, ErrInvalidWeight)
		err = r.AddNode("a", -10)
		assert.ErrorIs(t, err, ErrInvalidWeight)
	})
}

func TestRemoveNode(t *testing.T) {
	t.Run("should error on missing node", func(t *testing.T) {
		r := NewRing[string](strHasher)

		err := r.RemoveNode("missing")
		assert.ErrorIs(t, err, ErrNodeNotFound)
	})

	t.Run("should route keys away from removed node", func(t *testing.T) {
		r := NewRing[string](strHasher)

		err := r.AddNode("a", 50)
		assert.NoError(t, err)

		err = r.AddNode("b", 50)
		assert.NoError(t, err)

		err = r.RemoveNode("a")
		assert.NoError(t, err)

		for i := range 100 {
			got, err := r.Get(fmt.Sprintf("key-%d", i))
			assert.NoError(t, err)
			assert.Equal(t, "b", got)
		}
	})
}

func TestGet(t *testing.T) {
	t.Run("should error on empty ring", func(t *testing.T) {
		r := NewRing[string](strHasher)

		_, err := r.Get("key")
		assert.ErrorIs(t, err, ErrRingEmpty)
	})

	t.Run("should be deterministic", func(t *testing.T) {
		r := NewRing[string](strHasher)

		err := r.AddNode("a", 50)
		assert.NoError(t, err)
		err = r.AddNode("b", 50)
		assert.NoError(t, err)

		first, _ := r.Get("key-1")
		for range 100 {
			got, err := r.Get("key-1")

			assert.NoError(t, err)
			assert.Equal(t, first, got)
		}
	})

	t.Run("should distribute keys across all nodes", func(t *testing.T) {
		r := NewRing[string](strHasher)

		nodes := []string{"a", "b", "c"}
		for _, n := range nodes {
			_ = r.AddNode(n, 100)
		}

		counts := map[string]int{}
		for i := range 3000 {
			got, _ := r.Get(fmt.Sprintf("key-%d", i))
			counts[got]++
		}

		for _, n := range nodes {
			assert.Greater(t, counts[n], 0)
		}
	})

	t.Run("should give more keys to heavier node", func(t *testing.T) {
		r := NewRing[string](strHasher)

		_ = r.AddNode("light", 10)
		_ = r.AddNode("heavy", 200)

		counts := map[string]int{}
		for i := range 3000 {
			got, _ := r.Get(fmt.Sprintf("key-%d", i))
			counts[got]++
		}

		if counts["heavy"] <= counts["light"] {
			t.Fatalf("expected heavy > light: %v", counts)
		}
	})
}

func TestRemapping(t *testing.T) {
	t.Run("should remap only small fraction on node add", func(t *testing.T) {
		r := NewRing[string](strHasher)

		nodes := []string{"a", "b", "c", "d"}
		for _, n := range nodes {
			_ = r.AddNode(n, 100)
		}

		const keys = 3000
		before := make([]string, keys)
		for i := range keys {
			before[i], _ = r.Get(fmt.Sprintf("key-%d", i))
		}

		_ = r.AddNode("e", 100)

		moved := 0
		for i := range keys {
			got, _ := r.Get(fmt.Sprintf("key-%d", i))
			if got != before[i] {
				moved++
			}
		}

		// идеал ~1/5 = 600, даём запас до 40%
		if float64(moved)/keys > 0.4 {
			t.Fatalf("too many keys moved: %d/%d", moved, keys)
		}
	})
}

func TestUpdateNode(t *testing.T) {
	t.Run("should upsert missing node", func(t *testing.T) {
		r := NewRing[string](strHasher)
		if err := r.UpdateNode("a", 10); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if _, err := r.Get("key"); err != nil {
			t.Fatalf("expected node, got %v", err)
		}
	})

	t.Run("should rebalance on weight change", func(t *testing.T) {
		r := NewRing[string](strHasher)
		_ = r.AddNode("a", 10)
		_ = r.AddNode("b", 10)

		countsBefore := map[string]int{}
		for i := range 2000 {
			got, _ := r.Get(fmt.Sprintf("key-%d", i))
			countsBefore[got]++
		}

		if err := r.UpdateNode("a", 300); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		countsAfter := map[string]int{}
		for i := range 2000 {
			got, _ := r.Get(fmt.Sprintf("key-%d", i))
			countsAfter[got]++
		}

		if countsAfter["a"] <= countsBefore["a"] {
			t.Fatalf("expected a to grow: before=%v after=%v", countsBefore, countsAfter)
		}
	})
}

// запускать с -race
func TestConcurrent(t *testing.T) {
	r := NewRing[string](strHasher)
	_ = r.AddNode("init", 50)

	var wg sync.WaitGroup

	// readers
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 1000 {
				_, _ = r.Get(fmt.Sprintf("key-%d", j))
			}
		}()
		_ = i
	}

	// writers
	for i := range 4 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("node-%d", n)
			for j := range 10 {
				_ = r.UpdateNode(key, 10+j)
				_ = r.Nodes()
			}
		}(i)
	}

	// add/remove
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := range 10 {
			_ = r.AddNode(fmt.Sprintf("tmp-%d", j), 5)
			_ = r.RemoveNode(fmt.Sprintf("tmp-%d", j))
		}
	}()

	wg.Wait()
}

func BenchmarkGet(b *testing.B) {
	r := NewRing[string](strHasher)
	for i := range 10 {
		_ = r.AddNode(fmt.Sprintf("node-%d", i), 100)
	}

	b.ResetTimer()
	for i := range b.N {
		_, _ = r.Get(fmt.Sprintf("key-%d", i%10000))
	}
}

func BenchmarkGetParallel(b *testing.B) {
	r := NewRing[string](strHasher)
	for i := range 10 {
		_ = r.AddNode(fmt.Sprintf("node-%d", i), 100)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = r.Get(fmt.Sprintf("key-%d", i))
			i++
		}
	})
}

func BenchmarkAddNode(b *testing.B) {
	r := NewRing[string](strHasher)

	b.ResetTimer()
	for i := range b.N {
		_ = r.AddNode(fmt.Sprintf("node-%d", i), 10)
	}
}
