package hash

import (
	"fmt"
	"testing"
)

func TestSourcePartitionsCoverEveryKey(t *testing.T) {
	cases := [][2]int{{4, 8}, {8, 4}, {10, 16}, {6, 9}, {5, 7}, {1, 3}}
	for _, c := range cases {
		oldCount, newCount := c[0], c[1]
		sources := make(map[int]map[int]bool)
		for id := 0; id < newCount; id++ {
			sources[id] = make(map[int]bool)
			for _, s := range SourcePartitions(id, oldCount, newCount) {
				sources[id][s] = true
			}
		}
		for i := 0; i < 20000; i++ {
			k := fmt.Sprint("key-", i)
			if !sources[GetPartitionID(k, newCount)][GetPartitionID(k, oldCount)] {
				t.Fatalf("%d->%d: key %s lives in old partition %d, not listed as a source of %d",
					oldCount, newCount, k, GetPartitionID(k, oldCount), GetPartitionID(k, newCount))
			}
		}
	}
}

func TestSourcePartitionsDoubling(t *testing.T) {
	// Going from 4 to 8 partitions, each new partition is fed by exactly one old one.
	for id := 0; id < 8; id++ {
		if s := SourcePartitions(id, 4, 8); len(s) != 1 || s[0] != id%4 {
			t.Fatalf("SourcePartitions(%d, 4, 8) = %v", id, s)
		}
	}
}
