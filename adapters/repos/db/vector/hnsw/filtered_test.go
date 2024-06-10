package hnsw

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/hdf5"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func getHDF5ByteSize(dataset *hdf5.Dataset) uint {

	datatype, err := dataset.Datatype()
	if err != nil {
		log.Fatalf("Unabled to read datatype\n")
	}

	// log.WithFields(log.Fields{"size": datatype.Size()}).Printf("Parsing HDF5 byte format\n")
	byteSize := datatype.Size()
	if byteSize != 4 && byteSize != 8 {
		log.Fatalf("Unable to load dataset with byte size %d\n", byteSize)
	}
	return byteSize
}

func convert1DChunk[D float32 | float64](input []D, dimensions int, batchRows int) [][]float32 {
	chunkData := make([][]float32, batchRows)
	for i := range chunkData {
		chunkData[i] = make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			chunkData[i][j] = float32(input[i*dimensions+j])
		}
	}
	return chunkData
}

func loadHdf5Float32(file *hdf5.File, name string) [][]float32 {
	dataset, err := file.OpenDataset(name)
	if err != nil {
		log.Fatalf("Error opening loadHdf5Float32 dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	byteSize := getHDF5ByteSize(dataset)

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	rows := dims[0]
	dimensions := dims[1]

	var chunkData [][]float32

	if byteSize == 4 {
		chunkData1D := make([]float32, rows*dimensions)
		dataset.Read(&chunkData1D)
		chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(rows))
	} else if byteSize == 8 {
		chunkData1D := make([]float64, rows*dimensions)
		dataset.Read(&chunkData1D)
		chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(rows))
	}

	return chunkData
}

func loadHdf5Neighbors(file *hdf5.File, name string) [][]uint64 {
	dataset, err := file.OpenDataset(name)
	if err != nil {
		log.Fatalf("Error opening neighbors dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	rows := dims[0]
	dimensions := dims[1]

	byteSize := getHDF5ByteSize(dataset)

	chunkData := make([][]uint64, rows)

	if byteSize == 4 {
		chunkData1D := make([]int32, rows*dimensions)
		dataset.Read(&chunkData1D)
		for i := range chunkData {
			chunkData[i] = make([]uint64, dimensions)
			for j := uint(0); j < dimensions; j++ {
				chunkData[i][j] = uint64(chunkData1D[uint(i)*dimensions+j])
			}
		}
	} else if byteSize == 8 {
		chunkData1D := make([]uint64, rows*dimensions)
		dataset.Read(&chunkData1D)
		for i := range chunkData {
			chunkData[i] = chunkData1D[i*int(dimensions) : (i+1)*int(dimensions)]
		}
	}

	return chunkData
}

func train(file *hdf5.File) ([][]float32, error) {
	result := make([][]float32, 0, 100000)

	dataset, err := file.OpenDataset("train")
	if err != nil {
		log.Fatalf("Error opening dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	byteSize := getHDF5ByteSize(dataset)
	batchSize := uint(10000)

	rows := dims[0]
	dimensions := dims[1]

	memspace, err := hdf5.CreateSimpleDataspace([]uint{batchSize, dimensions}, []uint{batchSize, dimensions})
	if err != nil {
		return nil, err
	}
	defer memspace.Close()

	for i := uint(0); i < rows; i += batchSize {

		batchRows := batchSize
		// handle final smaller batch
		if i+batchSize > rows {
			batchRows = rows - i
			memspace, err = hdf5.CreateSimpleDataspace([]uint{batchRows, dimensions}, []uint{batchRows, dimensions})
			if err != nil {
				return nil, err
			}
		}

		offset := []uint{i, 0}
		count := []uint{batchRows, dimensions}

		err = dataspace.SelectHyperslab(offset, nil, count, nil)
		if err != nil {
			return nil, err
		}

		var chunkData [][]float32

		if byteSize == 4 {
			chunkData1D := make([]float32, batchRows*dimensions)

			err = dataset.ReadSubset(&chunkData1D, memspace, dataspace)
			if err != nil {
				return nil, err
			}

			chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(batchRows))

		} else if byteSize == 8 {
			chunkData1D := make([]float64, batchRows*dimensions)

			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(batchRows))

		}

		if (i+batchRows)%10000 == 0 {
			log.Printf("Imported %d/%d rows", i+batchRows, rows)
		}
		result = append(result, chunkData...)
	}
	return result, nil
}

func Test_Encoders(t *testing.T) {
	filterRate := 0.05
	path := "/Users/abdel/Documents/datasets/dbpedia-100k-openai-ada002.hdf5"
	file, err := hdf5.OpenFile(path, hdf5.F_ACC_RDONLY)
	assert.Nil(t, err)
	defer file.Close()
	data, _ := train(file)
	neighbors := loadHdf5Neighbors(file, "neighbors")
	testData := loadHdf5Float32(file, "test")
	fmt.Println(len(data[0]))
	labels := make([][]byte, len(data))
	for i := 0; i < len(data); i++ {
		if rand.Float64() < filterRate {
			labels[i] = []byte{0, 1}
		} else {
			labels[i] = []byte{0}
		}
	}

	logger := logrus.New()
	fmt.Printf("importing into hnsw\n")

	efConstruction := 256
	ef := []int{16, 32, 64, 128, 256, 512}
	maxNeighbors := 24

	uc := ent.UserConfig{
		MaxConnections: maxNeighbors,
		EFConstruction: efConstruction,
		EF:             ef[0],
	}

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "recallbenchmark",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return data[int(id)], nil
		},
	}, uc, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	before := time.Now()
	compressionhelpers.Concurrently(logger, uint64(len(data)), func(i uint64) {
		err := index.AddFiltered(uint64(i), data[i], labels[i])
		require.Nil(t, err)
	})
	fmt.Printf("importing took %s\n", time.Since(before))

	k := 10

	mutex := sync.Mutex{}
	for _, currEf := range ef {
		var relevant uint64
		var total int
		uc.EF = currEf
		index.UpdateUserConfig(uc, func() {})
		ellapsed := time.Duration(0)
		compressionhelpers.Concurrently(logger, uint64(len(testData)), func(i uint64) {
			before := time.Now()
			results, _, _ := index.SearchByVectorFiltered(testData[i], k, nil, 1)
			ell := time.Since(before)
			filteredNeighbors := make([]uint64, 0, k)
			j := 0
			for curr := 0; curr < len(neighbors[i]); curr++ {
				if len(labels[neighbors[i][curr]]) == 2 {
					filteredNeighbors = append(filteredNeighbors, neighbors[i][curr])
					j++
					if j == k {
						break
					}
				}
			}
			hits := MatchesInLists(filteredNeighbors, results)
			//hits := MatchesInLists(neighbors[i][:k], results)
			mutex.Lock()
			relevant += hits
			total += len(filteredNeighbors)
			ellapsed += ell
			mutex.Unlock()
		})

		recall := float32(relevant) / float32(total)
		latency := float32(ellapsed.Milliseconds()) / float32(len(testData))
		fmt.Print("ef: ", currEf, " -> ")
		fmt.Println(recall, latency, float32(total)/float32(len(testData)))
	}
	assert.Nil(t, index)
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
