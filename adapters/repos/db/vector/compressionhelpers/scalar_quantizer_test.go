//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build !race

package compressionhelpers

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func Test_NoRaceSQEncode(t *testing.T) {
	sq := NewScalarQuantizer([][]float32{
		{0, 0, 0, 0},
		{1, 1, 1, 1},
	}, distancer.NewCosineDistanceProvider())
	vec := []float32{0.5, 1, 0, 2}
	code := sq.Encode(vec)
	assert.NotNil(t, code)
	assert.Equal(t, byte(127), code[0])
	assert.Equal(t, byte(255), code[1])
	assert.Equal(t, byte(0), code[2])
	assert.Equal(t, byte(255), code[3])
	assert.Equal(t, uint16(255+255+127), sq.norm(code))
}

func Test_NoRaceSQDistance(t *testing.T) {
	distancers := []distancer.Provider{distancer.NewL2SquaredProvider(), distancer.NewCosineDistanceProvider(), distancer.NewDotProductProvider()}
	for _, distancer := range distancers {
		sq := NewScalarQuantizer([][]float32{
			{0, 0, 0, 0},
			{1, 1, 1, 1},
		}, distancer)
		vec1 := []float32{0.217, 0.435, 0, 0.348}
		vec2 := []float32{0.241, 0.202, 0.257, 0.300}

		dist, err := sq.DistanceBetweenCompressedVectors(sq.Encode(vec1), sq.Encode(vec2))
		expectedDist, _, _ := distancer.SingleDist(vec1, vec2)
		assert.Nil(t, err)
		if err == nil {
			assert.True(t, math.Abs(float64(expectedDist-dist)) < 0.01)
			fmt.Println(expectedDist-dist, expectedDist, dist)
		}
	}
}

func Test_NoRaceLASQEncode(t *testing.T) {
	sq := NewLocallyAdaptiveScalarQuantizer([][]float32{
		{0, 0, 0, 0},
		{1, 1, 1, 1},
	}, distancer.NewCosineDistanceProvider())
	vec := []float32{0.5, 1, 0, 2}
	code := sq.Encode(vec)
	assert.NotNil(t, code)
	assert.Equal(t, byte(127), code[0])
	assert.Equal(t, byte(255), code[1])
	assert.Equal(t, byte(0), code[2])
	assert.Equal(t, byte(255), code[3])
	assert.Equal(t, float32(0.0), sq.lowerBound(code))
	assert.Equal(t, float32(2.0), sq.upperBound(code))
}

type IndexAndDistance struct {
	index    uint64
	distance float32
}

func Test_NoRaceLASQDistance(t *testing.T) {
	distancers := []distancer.Provider{distancer.NewL2SquaredProvider(), distancer.NewCosineDistanceProvider(), distancer.NewDotProductProvider()}
	for _, distancer := range distancers {
		sq := NewLocallyAdaptiveScalarQuantizer([][]float32{
			{0, 0, 0, 0},
			{1, 1, 1, 1},
		}, distancer)
		vec1 := []float32{0.217, 0.435, 0, 0.348}
		vec2 := []float32{0.241, 0.202, 0.257, 0.300}

		dist, err := sq.DistanceBetweenCompressedVectors(sq.Encode(vec1), sq.Encode(vec2))
		expectedDist, _, _ := distancer.SingleDist(vec1, vec2)
		assert.Nil(t, err)
		if err == nil {
			assert.True(t, math.Abs(float64(expectedDist-dist)) < 0.00001)
			fmt.Println(expectedDist-dist, expectedDist, dist)
		}
	}
}

func Test_NoRaceLASQDistanceWithDistancer(t *testing.T) {
	distancers := []distancer.Provider{distancer.NewL2SquaredProvider(), distancer.NewCosineDistanceProvider(), distancer.NewDotProductProvider()}
	for _, distancer := range distancers {
		sq := NewLocallyAdaptiveScalarQuantizer([][]float32{
			{0, 0, 0, 0},
			{1, 1, 1, 1},
		}, distancer)
		vec1 := []float32{0.217, 0.435, 0, 0.348}
		vec2 := []float32{0.241, 0.202, 0.257, 0.300}

		distC, _ := sq.DistanceBetweenCompressedAndUncompressedVectors(vec1, sq.Encode(vec2))

		dist, err := sq.DistanceBetweenCompressedVectors(sq.Encode(vec1), sq.Encode(vec2))
		expectedDist, _, _ := distancer.SingleDist(vec1, vec2)
		assert.Nil(t, err)
		if err == nil {
			assert.True(t, math.Abs(float64(expectedDist-dist)) < 0.00001)
			fmt.Println(expectedDist-dist, expectedDist, dist, distC)
		}
	}
}

func distance(dp distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _, _ := dp.SingleDist(x, y)
		return dist
	}
}

func genVector(r *rand.Rand, dimensions int) []float32 {
	vector := make([]float32, 0, dimensions)
	for i := 0; i < dimensions; i++ {
		// Some distances like dot could produce negative values when the vectors have negative values
		// This change will not affect anything when using a distance like l2, but will cover some bugs
		// when using distances like dot
		vector = append(vector, r.Float32()*2-1)
	}
	return vector
}

func RandomVecs(size int, queriesSize int, dimensions int) ([][]float32, [][]float32) {
	fmt.Printf("generating %d vectors...\n", size+queriesSize)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	vectors := make([][]float32, 0, size)
	queries := make([][]float32, 0, queriesSize)
	for i := 0; i < size; i++ {
		vectors = append(vectors, genVector(r, dimensions))
	}
	for i := 0; i < queriesSize; i++ {
		queries = append(queries, genVector(r, dimensions))
	}
	return vectors, queries
}

type DistanceFunction func([]float32, []float32) float32

func BruteForce(logger logrus.FieldLogger, vectors [][]float32, query []float32, k int, distance DistanceFunction) ([]uint64, []float32) {
	type distanceAndIndex struct {
		distance float32
		index    uint64
	}

	distances := make([]distanceAndIndex, len(vectors))

	Concurrently(logger, uint64(len(vectors)), func(i uint64) {
		dist := distance(query, vectors[i])
		distances[i] = distanceAndIndex{
			index:    uint64(i),
			distance: dist,
		}
	})

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	if len(distances) < k {
		k = len(distances)
	}

	out := make([]uint64, k)
	dists := make([]float32, k)
	for i := 0; i < k; i++ {
		out[i] = distances[i].index
		dists[i] = distances[i].distance
	}

	return out, dists
}

func MatchesInLists(control []uint64, results []uint64) uint64 {
	desired := map[uint64]struct{}{}
	for _, relevant := range control {
		desired[relevant] = struct{}{}
	}

	var matches uint64
	for _, candidate := range results {
		_, ok := desired[candidate]
		if ok {
			matches++
		}
	}

	return matches
}

func Test_NoRaceLASQDistance2(t *testing.T) {
	distancers := []distancer.Provider{distancer.NewL2SquaredProvider(), distancer.NewCosineDistanceProvider(), distancer.NewDotProductProvider()}
	k := 10
	for _, distancer := range distancers {
		vecs, queries := RandomVecs(1000, 100, 25)
		sq := NewLocallyAdaptiveScalarQuantizer(vecs, distancer)
		encoded := make([][]byte, len(vecs))
		for i := 0; i < len(vecs); i++ {
			encoded[i] = sq.Encode(vecs[i])
		}
		relevant := uint64(0)
		for _, q := range queries {
			distances := make([]IndexAndDistance, len(vecs))
			truth, _ := BruteForce(logrus.New(), vecs, q, k, distance(distancer))
			//distancer := sq.NewDistancer(q)
			cq := sq.Encode(q)
			for v := range vecs {
				d, _ := sq.DistanceBetweenCompressedVectors(cq, encoded[v])
				distances[v] = IndexAndDistance{index: uint64(v), distance: d}
			}
			sort.Slice(distances, func(a, b int) bool {
				return distances[a].distance < distances[b].distance
			})

			results := make([]uint64, 0, k)
			for i := 0; i < k; i++ {
				results = append(results, distances[i].index)
			}
			relevant += MatchesInLists(truth, results)
		}

		recall := float32(relevant) / float32(k*len(queries))
		fmt.Println(recall)
		assert.True(t, recall > 0.99)
	}
}
