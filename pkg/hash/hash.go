package hash

import (
	"hash/fnv"
)

// Sum returns the 32-bit FNV-1a hash of key.
func Sum(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

// GetPartitionID implements hash(key) % partitionCount.
func GetPartitionID(key string, partitionCount int) int {
	if partitionCount <= 0 {
		return 0
	}
	return int(Sum(key) % uint32(partitionCount))
}

// SourcePartitions returns the partitions of an oldCount layout that may hold keys which map to
// newID under a newCount layout. h%oldCount == p and h%newCount == newID have a common solution
// iff p ≡ newID (mod gcd(oldCount, newCount)), so only those partitions need to be copied from.
func SourcePartitions(newID, oldCount, newCount int) []int {
	g := gcd(oldCount, newCount)
	var sources []int
	for p := 0; p < oldCount; p++ {
		if p%g == newID%g {
			sources = append(sources, p)
		}
	}
	return sources
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
