package decimalext

// ExtendBytesArrayCapacity extends dst capacity to hold additionalItems
// and returns the extended dst.
func ExtendBytesArrayCapacity(dst [][]byte, additionalItems int) [][]byte {
	dstLen := len(dst)
	if n := dstLen + additionalItems - cap(dst); n > 0 {
		dst = append(dst[:cap(dst)], make([][]byte, n)...)
	}
	return dst[:dstLen]
}
