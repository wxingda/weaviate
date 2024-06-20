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

package compressionhelpers

var l2SquaredByteImpl func(a, b []byte) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		diff := uint32(a[i]) - uint32(b[i])
		sum += diff * diff
	}

	return sum
}

var dotByteImpl func(a, b []uint8) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		sum += uint32(a[i]) * uint32(b[i])
	}

	return sum
}

var dotPQByteImpl func(uncompressed []float32, compressed []uint8, codebook [][][]float32) float32 = func(uncompressed []float32, compressed []byte, codebook [][][]float32) float32 {
	var sum float32

	segmenLen := len(uncompressed) / len(compressed)
	for i := range compressed {
		segment := i / segmenLen
		positionInSegment := i % segmenLen
		sum += uncompressed[i] * codebook[segment][compressed[segment]][positionInSegment]
	}

	return sum
}
