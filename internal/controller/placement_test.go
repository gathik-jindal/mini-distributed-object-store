package controller

import (
	"testing"
)

var four = []string{"worker1", "worker2", "worker3", "worker4"}

func TestRankDeterministicAndDistinct(t *testing.T) {
	a := RankWorkers("abc-c00001", four)
	b := RankWorkers("abc-c00001", []string{"worker4", "worker3", "worker2", "worker1"})
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("not deterministic: %v vs %v", a, b)
		}
	}
	if len(a) != 4 || a[0] == a[1] {
		t.Fatalf("bad ranking %v", a)
	}
}

func TestEvenSpread(t *testing.T) {
	counts := map[string]int{}
	for i := 0; i < 1000; i++ {
		for _, w := range RankWorkers(ChunkIDFor("obj", i), four)[:2] {
			counts[w]++
		}
	}
	for w, n := range counts { // 2000 replicas over 4 workers -> ~500 each
		if n < 400 || n > 600 {
			t.Fatalf("uneven spread %s=%d (%v)", w, n, counts)
		}
	}
}

func TestMinimalMovement(t *testing.T) {
	three := []string{"worker1", "worker2", "worker3"}
	for i := 0; i < 1000; i++ {
		cid := ChunkIDFor("obj", i)
		before, after := RankWorkers(cid, four)[:2], RankWorkers(cid, three)[:2]
		if before[0] != "worker4" && before[1] != "worker4" && (before[0] != after[0] || before[1] != after[1]) {
			t.Fatalf("chunk %d moved although its workers survived", i)
		}
	}
}

func TestLeastLoaded(t *testing.T) {
	if got := LeastLoaded([]string{"a", "b", "c"}, map[string]int64{"a": 5, "b": 1, "c": 3}, "x")[0]; got != "b" {
		t.Fatal(got)
	}
}

func TestChunkIDRoundTrip(t *testing.T) {
	oid := "0c1d2e3f-cccc-4ccc-8ccc-cccccccccccc"
	cid := ChunkIDFor(oid, 42)
	if cid != oid+"-c00042" || ObjectIDOf(cid) != oid {
		t.Fatal(cid, ObjectIDOf(cid))
	}
}
