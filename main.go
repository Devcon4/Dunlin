package main

import (
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"math/rand"
	"sync"
	"time"
)

func main() {
	record, count := Hasher(5, 1000000)
	rand.Seed(time.Now().UnixNano())

	recordCount := 1 << 24
	fmt.Println(recordCount)

	for i := 0; i < recordCount; i++ {
		record(randLetters(3))
	}

	searchTerm := randLetters(3)
	fmt.Println(searchTerm)
	fmt.Println(count(searchTerm))
}

// randLetters creates a random string for the given length.
func randLetters(length int) string {
	runes := []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, length)
	for i := range b {
		b[i] = runes[rand.Intn(len(runes))]
	}
	return string(b)
}

// makeRecordFunc creates a record func, used with hasher.
func makeRecordFunc(matrix []map[[10]byte]int, matrixMutex *sync.Mutex, seeds []maphash.Seed, wg *sync.WaitGroup) func(term string) {
	return func(term string) {
		// Use a waitGroup and inner closure to parallelize work.
		wg.Add(1)

		go func() {
			defer wg.Done()
			// Iterate hash funcs.
			for i, seed := range seeds {
				// Create maphash with seed.
				hashFunc := &maphash.Hash{}
				hashFunc.SetSeed(seed)
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
				// Use mutex to lock matrix accross goroutines.
				matrixMutex.Lock()
				matrix[i][btFixed]++
				matrixMutex.Unlock()
			}
		}()
	}
}

// makeCountFunc creates a count func, used with hasher.
func makeCountFunc(matrix []map[[10]byte]int, matrixMutex *sync.Mutex, seeds []maphash.Seed, wg *sync.WaitGroup, depth int) func(term string) int {
	return func(term string) int {
		// Splice to store each hashFuncs count.
		hashes := make([]int, depth)

		// Wait for the workgroup to be cleared before counting.
		// This ensures count happens after all record goroutines have ran.
		wg.Wait()
		// Loop the hashFuncs.
		for i, seed := range seeds {
			// Create maphash with seed.
			hashFunc := &maphash.Hash{}
			hashFunc.SetSeed(seed)
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
			matrixMutex.Lock()
			hashes[i] = matrix[i][btFixed]
			matrixMutex.Unlock()
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
	matrixMutex := &sync.Mutex{}
	// Create waitGroup to sync record and count tasks.
	wg := &sync.WaitGroup{}

	// Create matrix maps.
	matrix := make([]map[[10]byte]int, length)
	for i := range matrix {
		matrix[i] = make(map[[10]byte]int)
	}

	// Setup list of seeds to use for each hash function.
	seeds := genSeeds(depth)

	// Record adds a string to the sketch table.
	record = makeRecordFunc(matrix, matrixMutex, seeds, wg)

	// Count returns the number of occurences a term appears in the sketch.
	count = makeCountFunc(matrix, matrixMutex, seeds, wg, depth)

	return record, count
}

// genSeeds Creates a splice of maphash seeds.
func genSeeds(length int) []maphash.Seed {
	list := make([]maphash.Seed, length)

	for i := range list {
		list[i] = maphash.MakeSeed()
	}

	return list
}
