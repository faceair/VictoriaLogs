package encodingext

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/decimalext"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// MarshalType is the type used for the marshaling.
type MarshalType = encoding.MarshalType

const (
	// MarshalTypeZSTDBytesArray is used for marshaling bytes array
	MarshalTypeZSTDBytesArray = MarshalType(7)
)

// CheckMarshalType verifies whether the mt is valid.
func CheckMarshalType(mt MarshalType) error {
	if mt < 0 || mt > 8 {
		return fmt.Errorf("MarshalType should be in range [0..8]; got %d", mt)
	}
	return nil
}

// MarshalValues marshals values, appends the marshaled result to dst
// and returns the dst.
//
// precisionBits must be in the range [1...64], where 1 means 50% precision,
// while 64 means 100% precision, i.e. lossless encoding.
func MarshalValues(dst []byte, values [][]byte) (result []byte, mt MarshalType) {
	return marshalBytesArray(dst, values)
}

// UnmarshalValues unmarshals values from src, appends them to dst and returns
// the resulting dst.
//
// firstValue must be the value returned from MarshalValues.
func UnmarshalValues(dst [][]byte, src []byte, mt MarshalType, itemsCount int) ([][]byte, error) {
	dst, err := unmarshalBytesArray(dst, src, mt, itemsCount)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal %d values from len(src)=%d bytes: %w", itemsCount, len(src), err)
	}
	return dst, nil
}

func marshalBytesArray(dst []byte, a [][]byte) (result []byte, mt MarshalType) {
	if len(a) == 0 {
		logger.Panicf("BUG: a must contain at least one item")
	}
	bb := bbPool.Get()

	for i := 0; i < len(a); i++ {
		bb.B = encoding.MarshalBytes(bb.B, a[i])
	}
	dst = encoding.CompressZSTDLevel(dst, bb.B, getCompressLevel(len(bb.B)))

	bbPool.Put(bb)
	return dst, MarshalTypeZSTDBytesArray
}

func unmarshalBytesArray(dst [][]byte, src []byte, mt MarshalType, itemsCount int) ([][]byte, error) {
	// Extend dst capacity in order to eliminate memory allocations below.
	dst = decimalext.ExtendBytesArrayCapacity(dst, itemsCount)

	switch mt {
	case MarshalTypeZSTDBytesArray:
		bb := bbPool.Get()
		defer bbPool.Put(bb)

		var err error

		bb.B, err = encoding.DecompressZSTD(bb.B[:0], src)
		if err != nil {
			return nil, fmt.Errorf("cannot decompress zstd data: %w", err)
		}

		var b []byte
		for i := 0; i < itemsCount; i++ {
			bb.B, b, err = encoding.UnmarshalBytes(bb.B)
			if err != nil {
				return nil, err
			}
			dst = append(dst, append([]byte(nil), b...))
		}
	default:
		return nil, fmt.Errorf("unknown MarshalType=%d", mt)
	}

	return dst, nil
}

var bbPool bytesutil.ByteBufferPool

func getCompressLevel(itemsCount int) int {
	if itemsCount <= 1<<6 {
		return 1
	}
	if itemsCount <= 1<<8 {
		return 2
	}
	if itemsCount <= 1<<10 {
		return 3
	}
	if itemsCount <= 1<<12 {
		return 4
	}
	return 5
}
