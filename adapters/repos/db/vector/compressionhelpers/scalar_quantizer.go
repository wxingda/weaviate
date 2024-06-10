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

import (
	"encoding/binary"
	"math"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
)

const (
	codes     = 255.0
	codes2    = codes * codes
	codesLasq = 255.0
)

var l2SquaredByteImpl func(a, b []byte) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		diff := uint32(a[i]) - uint32(b[i])
		sum += diff * diff
	}

	return sum
}

var dotByteImpl func(a, b []uint8) uint32 = asm.DotByteARM64

/*var dotByteImpl func(a, b []uint8) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		sum += uint32(a[i]) * uint32(b[i])
	}

	return sum
}*/

var dotFloatByteImpl func(a []float32, b []uint8) float32 = func(a []float32, b []uint8) float32 {
	var sum float32

	for i := range a {
		sum += a[i] * float32(b[i])
	}

	return sum
}

var LAQDotImpl func(x []float32, y []byte) float32 = func(x []float32, y []byte) float32 {
	sum := float32(0)
	for i := range x {
		sum += x[i] * float32(y[i])
	}
	return sum
}

type ScalarQuantizer struct {
	a         float32
	b         float32
	a2        float32
	ab        float32
	ib2       float32
	distancer distancer.Provider
}

func (sq *ScalarQuantizer) DistanceBetweenCompressedVectors(x, y []byte) (float32, error) {
	if len(x) != len(y) {
		return 0, errors.Errorf("vector lengths don't match: %d vs %d",
			len(x), len(y))
	}
	switch sq.distancer.Type() {
	case "l2-squared":
		return sq.a2 * float32(l2SquaredByteImpl(x[:len(x)-4], y[:len(y)-4])), nil
	case "dot":
		return -(sq.a2*float32(dotByteImpl(x[:len(x)-4], y[:len(y)-4])) + sq.ab*float32(sq.norm(x)+sq.norm(y)) + sq.ib2), nil
	case "cosine-dot":
		return 1 - (sq.a2*float32(dotByteImpl(x[:len(x)-4], y[:len(y)-4])) + sq.ab*float32(sq.norm(x)+sq.norm(y)) + sq.ib2), nil
	}
	return 0, errors.Errorf("Distance not supported yet %s", sq.distancer)
}

func (sq *ScalarQuantizer) DistanceBetweenCompressedAndUncompressedVectors2(x []float32, encoded []byte, normX, normX2 float32) (float32, error) {
	switch sq.distancer.Type() {
	case "l2-squared":
		return normX2, nil
	case "dot":
		return -(sq.a/codes*float32(dotFloatByteImpl(x, encoded[:len(encoded)-4])) + sq.b*normX), nil
	}
	return 0, errors.Errorf("Distance not supported yet %s", sq.distancer)
}

func (sq *ScalarQuantizer) DistanceBetweenCompressedAndUncompressedVectors(x []float32, encoded []byte) (float32, error) {
	switch sq.distancer.Type() {
	case "dot":
		return -(sq.a/codes*float32(dotFloatByteImpl(x, encoded[:len(encoded)-4])) + sq.b*float32(sq.norm(encoded))), nil
	}
	return 0, errors.Errorf("Distance not supported yet %s", sq.distancer)
}

func NewScalarQuantizer(data [][]float32, distance distancer.Provider) *ScalarQuantizer {
	if len(data) == 0 {
		return nil
	}

	sq := &ScalarQuantizer{
		distancer: distance,
	}
	sq.b = data[0][0]
	for i := 1; i < len(data); i++ {
		vec := data[i]
		for _, x := range vec {
			if x < sq.b {
				sq.a += sq.b - x
				sq.b = x
			} else if x-sq.b > sq.a {
				sq.a = x - sq.b
			}
		}
	}
	sq.a2 = sq.a * sq.a / codes2
	sq.ab = sq.a * sq.b / codes
	sq.ib2 = sq.b * sq.b * float32(len(data[0]))
	return sq
}

func codeFor(x, a, b, codes float32) byte {
	if x < b {
		return 0
	} else if x-b > a {
		return byte(codes)
	} else {
		return byte(math.Floor(float64((x - b) * codes / a)))
	}
}

func (sq *ScalarQuantizer) Encode(vec []float32) []byte {
	var sum uint32 = 0
	code := make([]byte, len(vec)+4)
	for i := 0; i < len(vec); i++ {
		code[i] = codeFor(vec[i], sq.a, sq.b, codes)
		sum += uint32(code[i])
	}
	binary.BigEndian.PutUint32(code[len(vec):], sum)
	return code
}

type SQDistancer struct {
	x          []float32
	normX2     float32
	sq         *ScalarQuantizer
	compressed []byte
	normX      float32
}

func (sq *ScalarQuantizer) NewDistancer(a []float32) *SQDistancer {
	sum := float32(0)
	sum2 := float32(0)
	for _, x := range a {
		sum += x
		sum2 += (x * x)
	}
	return &SQDistancer{
		x:          a,
		sq:         sq,
		compressed: sq.Encode(a),
		normX:      sum,
		normX2:     sum2,
	}
}

func (d *SQDistancer) Distance(x []byte) (float32, bool, error) {
	dist, err := d.sq.DistanceBetweenCompressedAndUncompressedVectors2(d.x, x, d.normX, d.normX2)
	return dist, err == nil, err
}

func (d *SQDistancer) DistanceToFloat(x []float32) (float32, bool, error) {
	if len(d.x) > 0 {
		return d.sq.distancer.SingleDist(d.x, x)
	}
	xComp := d.sq.Encode(x)
	dist, err := d.sq.DistanceBetweenCompressedVectors(d.compressed, xComp)
	return dist, err == nil, err
}

func (sq *ScalarQuantizer) NewQuantizerDistancer(a []float32) quantizerDistancer[byte] {
	return sq.NewDistancer(a)
}

func (sq *ScalarQuantizer) NewCompressedQuantizerDistancer(a []byte) quantizerDistancer[byte] {
	return &SQDistancer{
		x:          nil,
		sq:         sq,
		compressed: a,
	}
}

func (sq *ScalarQuantizer) ReturnQuantizerDistancer(distancer quantizerDistancer[byte]) {}

func (sq *ScalarQuantizer) CompressedBytes(compressed []byte) []byte {
	return compressed
}

func (sq *ScalarQuantizer) FromCompressedBytes(compressed []byte) []byte {
	return compressed
}

func (sq *ScalarQuantizer) ExposeFields() PQData {
	return PQData{}
}

func (sq *ScalarQuantizer) norm(code []byte) uint32 {
	return binary.BigEndian.Uint32(code[len(code)-4:])
}

type LaScalarQuantizer struct {
	distancer distancer.Provider
	dims      int
	means     []float32
	meansAcc  float32
}

func NewLocallyAdaptiveScalarQuantizer(data [][]float32, distance distancer.Provider) *LaScalarQuantizer {
	dims := len(data[0])
	means := make([]float32, dims)
	meansAcc := float32(0)
	for _, v := range data {
		for i := range v {
			means[i] += v[i]
		}
	}
	for i := range data[0] {
		means[i] /= float32(len(data))
		meansAcc += means[i]
	}
	return &LaScalarQuantizer{
		distancer: distance,
		dims:      dims,
		means:     means,
		meansAcc:  meansAcc,
	}
}

func (lasq *LaScalarQuantizer) Encode(vec []float32) []byte {
	min, max := float32(math.MaxFloat32), float32(-math.MaxFloat32)
	for i, x := range vec {
		corrected := x - lasq.means[i]
		if min > corrected {
			min = corrected
		}
		if max < corrected {
			max = corrected
		}
	}
	code := make([]byte, len(vec)+12)

	var sum uint32 = 0
	for i := 0; i < len(vec); i++ {
		code[i] = codeFor(vec[i]-lasq.means[i], max-min, min, codesLasq)
		sum += uint32(code[i])
	}
	binary.BigEndian.PutUint32(code[len(vec):], sum)
	binary.BigEndian.PutUint32(code[len(vec)+4:], math.Float32bits(min))
	binary.BigEndian.PutUint32(code[len(vec)+8:], math.Float32bits(max))
	return code
}

func (lasq *LaScalarQuantizer) Decode(x []byte) []float32 {
	bx := lasq.lowerBound(x)
	ax := (lasq.upperBound(x) - bx) / codesLasq
	correctedX := make([]float32, lasq.dims)
	for i := 0; i < lasq.dims; i++ {
		correctedX[i] = float32(x[i])*ax + bx + lasq.means[i]
	}
	return correctedX
}

func (lasq *LaScalarQuantizer) decodeOne(x byte, lower, correctedRange, mean float32) float32 {
	return float32(x)*correctedRange + lower + mean
}

func (lasq *LaScalarQuantizer) DistanceBetweenCompressedVectors(x, y []byte) (float32, error) {
	if len(x) != len(y) {
		return 0, errors.Errorf("vector lengths don't match: %d vs %d",
			len(x), len(y))
	}

	bx := lasq.lowerBound(x)
	ax := (lasq.upperBound(x) - bx) / codesLasq
	by := lasq.lowerBound(y)
	ay := (lasq.upperBound(y) - by) / codesLasq
	xNorm := float32(lasq.norm(x))
	yNorm := float32(lasq.norm(y))
	x = x[:lasq.dims]
	y = y[:lasq.dims]
	sum := ax*ay*float32(dotByteImpl(x, y)) + ax*by*xNorm + ay*bx*yNorm + ax*LAQDotImpl(lasq.means, x) + ay*LAQDotImpl(lasq.means, y) + (bx+by)*lasq.meansAcc + float32(len(x))*bx*by
	return -sum, nil
}

func (lasq *LaScalarQuantizer) DistanceBetweenCompressedAndUncompressedVectors(x []float32, y []byte) (float32, error) {
	by := lasq.lowerBound(y)
	ay := (lasq.upperBound(y) - by) / codesLasq

	sum := float32(0)
	for i := range x {
		sum += x[i] * (float32(y[i])*ay + by + lasq.means[i])
	}
	return -sum, nil
}

func (lasq *LaScalarQuantizer) DistanceBetweenCompressedAndUncompressedVectors2(x []float32, y []byte, xNorm, meanProd float32) (float32, error) {
	by := lasq.lowerBound(y)
	ay := (lasq.upperBound(y) - by) / codesLasq

	sum := LAQDotImpl(x, y)
	return -sum*ay - xNorm*by - meanProd, nil
}

func (lasq *LaScalarQuantizer) lowerBound(code []byte) float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(code[lasq.dims+4:]))
}

func (lasq *LaScalarQuantizer) upperBound(code []byte) float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(code[lasq.dims+8:]))
}

func (lasq *LaScalarQuantizer) norm(code []byte) uint32 {
	return binary.BigEndian.Uint32(code[lasq.dims:])
}

type LASQDistancer struct {
	x          []float32
	norm       float32
	meanProd   float32
	sq         *LaScalarQuantizer
	compressed []byte
}

func (sq *LaScalarQuantizer) NewDistancer(a []float32) *LASQDistancer {
	sum := float32(0)
	meanProd := float32(0)
	for i, xi := range a {
		sum += xi
		meanProd += xi * sq.means[i]
	}
	return &LASQDistancer{
		x:          a,
		sq:         sq,
		norm:       sum,
		meanProd:   meanProd,
		compressed: sq.Encode(a),
	}
}

func (d *LASQDistancer) Distance(x []byte) (float32, bool, error) {
	dist, err := d.sq.DistanceBetweenCompressedAndUncompressedVectors2(d.x, x, d.norm, d.meanProd)
	return dist, err == nil, err
}

func (d *LASQDistancer) DistanceToFloat(x []float32) (float32, bool, error) {
	if len(d.x) > 0 {
		return d.sq.distancer.SingleDist(d.x, x)
	}
	xComp := d.sq.Encode(x)
	dist, err := d.sq.DistanceBetweenCompressedVectors(d.compressed, xComp)
	return dist, err == nil, err
}

func (sq *LaScalarQuantizer) NewQuantizerDistancer(a []float32) quantizerDistancer[byte] {
	return sq.NewDistancer(a)
}

func (sq *LaScalarQuantizer) NewCompressedQuantizerDistancer(a []byte) quantizerDistancer[byte] {
	return &LASQDistancer{
		x:          nil,
		sq:         sq,
		compressed: a,
	}
}

func (sq *LaScalarQuantizer) ReturnQuantizerDistancer(distancer quantizerDistancer[byte]) {}

func (sq *LaScalarQuantizer) CompressedBytes(compressed []byte) []byte {
	return compressed
}

func (sq *LaScalarQuantizer) FromCompressedBytes(compressed []byte) []byte {
	return compressed
}

func (sq *LaScalarQuantizer) ExposeFields() PQData {
	return PQData{}
}

type TScalarQuantizer struct {
	distancer distancer.Provider
	dims      int
	tiles     []*TileEncoder
}

func NewTilesScalarQuantizer(data [][]float32, distance distancer.Provider) *TScalarQuantizer {
	dims := len(data[0])

	quantizer := &TScalarQuantizer{
		distancer: distance,
		dims:      dims,
		tiles:     nil,
	}

	for i := 0; i < dims; i++ {
		quantizer.tiles = append(quantizer.tiles, NewTileEncoder(8, i, NormalEncoderDistribution))
	}
	for _, d := range data {
		for i := 0; i < dims; i++ {
			quantizer.tiles[i].Add(d)
		}
	}
	for i := 0; i < dims; i++ {
		quantizer.tiles[i].Fit(nil)
	}

	return quantizer
}

func (sq *TScalarQuantizer) Encode(x []float32) []byte {
	res := make([]byte, len(x))
	for i := 0; i < sq.dims; i++ {
		res[i] = sq.tiles[i].Encode(x)
	}
	return res
}

func (sq *TScalarQuantizer) DistanceBetweenCompressedVectors(x, y []byte) (float32, error) {
	decX := make([]float32, len(x))
	decY := make([]float32, len(y))
	for i := 0; i < sq.dims; i++ {
		decX[i] = sq.tiles[i].Centroid(x[i])[0]
		decY[i] = sq.tiles[i].Centroid(y[i])[0]
	}
	res, _, err := sq.distancer.SingleDist(decX, decY)
	return res, err
}

func (sq *TScalarQuantizer) DistanceBetweenCompressedAndUncompressedVectors(x []float32, y []byte) (float32, error) {
	decY := make([]float32, len(y))
	for i := 0; i < sq.dims; i++ {
		decY[i] = sq.tiles[i].Centroid(y[i])[0]
	}
	res, _, err := sq.distancer.SingleDist(x, decY)
	return res, err
}
