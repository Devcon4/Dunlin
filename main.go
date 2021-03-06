package main

import (
	"encoding/binary"
	"fmt"
	"hash/maphash"
)

func main() {
	record, count := Hasher(5, 1000000)

	record("test")
	record("test333")
	record("test333")
	record("test333")
	record("test333")
	record("test333")
	record("test333")

	fmt.Println(count("test333"))
}

func makeRecordFunc(matrix []map[[10]byte]int, hashTable []maphash.Hash) func(term string) {
	return func(term string) {
		// Iterate hash funcs.
		for i, hashFunc := range hashTable {
			// Reset hash so it's fresh.
			hashFunc.Reset()
			// Add the search term to the hash builder.
			hashFunc.WriteString(term)
			// Gen new hash.
			hash := hashFunc.Sum64()

			// You can't have splices as a key to a map but you can have an array.
			// This is a hack to copy the hash uint64 to a []byte splice then copy that splice into an array.
			bt := make([]byte, 10)
			var btFixed [10]byte
			binary.PutUvarint(bt, hash)
			copy(btFixed[:], bt)

			// Increment count at hash index.
			matrix[i][btFixed]++
		}
	}
}

func makeCountFunc(matrix []map[[10]byte]int, hashTable []maphash.Hash, depth int) func(term string) int {
	return func(term string) int {
		// Splice to store each hashFuncs count.
		hashes := make([]int, depth)

		// Loop the hashFuncs.
		for i, hashFunc := range hashTable {
			// Reset the hashFunc so it's fresh.
			hashFunc.Reset()
			// Add the search term to the hash builder.
			hashFunc.WriteString(term)
			// Gen new hash.
			hash := hashFunc.Sum64()

			// You can't have splices as a key to a map but you can have an array.
			// This is a hack to copy the hash uint64 to a []byte splice then copy that splice into an array.
			bt := make([]byte, 10)
			var btFixed [10]byte
			binary.PutUvarint(bt, hash)
			copy(btFixed[:], bt)

			// Grab the count for the hash from the sketch table and add it to the hashes splice.
			hashes[i] = matrix[i][btFixed]
		}

		// Find the min count in the hashes array.
		min := 0
		for i, e := range hashes {
			if i == 0 || e < min {
				min = e
			}
		}
		return min
	}
}

// Hasher Creates a Count min sketch table.
// Returns two funcs, One to add to the table and another to get counts from that table.
func Hasher(depth int, length int) (record func(string), count func(string) int) {
	// Create array of maps to store hash occurences.
	matrix := make([]map[[10]byte]int, length)
	for i := range matrix {
		matrix[i] = make(map[[10]byte]int)
	}

	// Setup list of seeds to use for each hash function.
	seeds := genSeeds(depth)
	// Create an array of hash funcs using our seeds.
	hashTable := genHashTable(seeds)

	// Record adds a string to the sketch table.
	record = makeRecordFunc(matrix, hashTable)

	// Count returns the number of occurences a term appears in the sketch.
	count = makeCountFunc(matrix, hashTable, depth)

	return record, count
}

func genHashTable(seeds []maphash.Seed) []maphash.Hash {
	table := make([]maphash.Hash, len(seeds))

	for i, seed := range seeds {
		table[i].SetSeed(seed)
	}

	return table
}

func genSeeds(length int) []maphash.Seed {
	list := make([]maphash.Seed, length)

	for i := range list {
		list[i] = maphash.MakeSeed()
	}

	return list
}
