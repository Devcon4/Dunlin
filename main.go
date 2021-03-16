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
	pipe := Hasher(5, 1000000)

	record := pipe(makeRecordFunc)
	count := pipe(makeCountFunc)

	// example of extending Hasher with a new custom operator that returns the matrixMutex.
	getMatrixMutex := pipe(func(options HasherOptions) func(term string) interface{} {
		return func(term string) interface{} {
			return options.matrixMutex
		}
	})

	// because go doesn't have generic types yet we have to still pass a term even though this func doesn't use it.
	mut := getMatrixMutex("").(*sync.Mutex)
	fmt.Println(mut)

	rand.Seed(time.Now().UnixNano())

	recordCount := 1 << 21
	fmt.Println(recordCount)

	for i := 0; i < recordCount; i++ {
		record(randLetters(3))
	}

	searchTerm := randLetters(3)
	fmt.Println(searchTerm)
	fmt.Println(count(searchTerm).(int))
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
func makeRecordFunc(options HasherOptions) func(term string) interface{} {
	return func(term string) interface{} {
		// Use a waitGroup and inner closure to parallelize work.
		options.wg.Add(1)

		go func() {
			defer options.wg.Done()
			// Iterate hash funcs.
			for i, seed := range options.seeds {
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
				options.matrixMutex.Lock()
				options.matrix[i][btFixed]++
				options.matrixMutex.Unlock()
			}
		}()
		return nil
	}
}

// makeCountFunc creates a count func, used with hasher.
func makeCountFunc(options HasherOptions) func(term string) interface{} {
	return func(term string) interface{} {
		// Splice to store each hashFuncs count.
		hashes := make([]int, options.depth)

		// Wait for the workgroup to be cleared before counting.
		// This ensures count happens after all record goroutines have ran.
		options.wg.Wait()
		// Loop the hashFuncs.
		for i, seed := range options.seeds {
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
			options.matrixMutex.Lock()
			hashes[i] = options.matrix[i][btFixed]
			options.matrixMutex.Unlock()
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

// HasherOptions is a struct to make passing hasher state easier.
type HasherOptions struct {
	matrix      []map[[10]byte]int
	matrixMutex *sync.Mutex
	seeds       []maphash.Seed
	wg          *sync.WaitGroup
	depth       int
}

// makePipeFunc takes a func and provides HasherOptions to it. Used to create new operator funcs on Hasher.
func makePipeFunc(options HasherOptions) func(func(HasherOptions) func(term string) interface{}) func(term string) interface{} {
	return func(operator func(HasherOptions) func(term string) interface{}) func(term string) interface{} {
		return operator(options)
	}
}

// Hasher Creates a Count min sketch table.
// Returns two funcs, One to add to the table and another to get counts from that table.
func Hasher(depth int, length int) (pipe func(func(HasherOptions) func(term string) interface{}) func(term string) interface{}) {
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

	options := &HasherOptions{
		matrix:      matrix,
		matrixMutex: matrixMutex,
		seeds:       seeds,
		wg:          wg,
		depth:       depth,
	}

	return makePipeFunc(*options)
}

// genSeeds Creates a splice of maphash seeds.
func genSeeds(length int) []maphash.Seed {
	list := make([]maphash.Seed, length)

	for i := range list {
		list[i] = maphash.MakeSeed()
	}

	return list
}
