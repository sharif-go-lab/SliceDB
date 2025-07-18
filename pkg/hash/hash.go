package hash

import (
	"hash/fnv"
)

// GetPartitionID calculates which partition a key belongs to
func GetPartitionID(key string, partitionCount int) int {
	h := fnv.New32a()
	if _, err := h.Write([]byte(key)); err != nil {
		panic(err)
	}
	return int(h.Sum32()) % partitionCount
}
