package db

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"

	pq "gitee.com/opengauss/openGauss-connector-go-pq"
)

func PQArray(a interface{}) interface {
	driver.Valuer
	sql.Scanner
} {
	switch a := a.(type) {
	case []bool:
		return (*pq.BoolArray)(&a)
	case []float64:
		return (*pq.Float64Array)(&a)
	case []float32:
		return (*pq.Float32Array)(&a)
	case []int64:
		return (*pq.Int64Array)(&a)
	case []int32:
		return (*pq.Int32Array)(&a)
	case []string:
		return (*pq.StringArray)(&a)
	case [][]byte:
		return (*pq.ByteaArray)(&a)
	case *[]bool:
		return (*pq.BoolArray)(a)
	case *[]float64:
		return (*pq.Float64Array)(a)
	case *[]float32:
		return (*pq.Float32Array)(a)
	case *[]int64:
		return (*pq.Int64Array)(a)
	case *[]int32:
		return (*pq.Int32Array)(a)
	case *[]string:
		return (*pq.StringArray)(a)
	case *[][]byte:
		return (*pq.ByteaArray)(a)
	case []uint32:
		return (*Uint32Array)(&a)
	case *[]uint32:
		return (*Uint32Array)(a)
	case []uint64:
		return (*Uint64Array)(&a)
	case *[]uint64:
		return (*Uint64Array)(a)
	default:
		return pq.GenericArray{a}
	}
}

type Uint64Array []uint64

// Scan implements the sql.Scanner interface.
func (a *Uint64Array) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		return a.scanBytes(src)
	case string:
		return a.scanBytes([]byte(src))
	case nil:
		*a = nil
		return nil
	}

	return fmt.Errorf("pq: cannot convert %T to Uint64Array", src)
}

func (a *Uint64Array) scanBytes(src []byte) error {
	elems, err := scanLinearArray(src, []byte{','}, "Uint64Array")
	if err != nil {
		return err
	}

	if *a != nil && len(elems) == 0 {
		*a = (*a)[:0]
	} else {
		b := make(Uint64Array, len(elems))
		for i, v := range elems {
			if b[i], err = strconv.ParseUint(string(v), 10, 64); err != nil {
				return fmt.Errorf("pq: parsing array element index %d: %v", i, err)
			}
		}

		*a = b
	}

	return nil
}

// Value implements the driver.Valuer interface.
func (a Uint64Array) Value() (driver.Value, error) {
	n := len(a)
	if n == 0 {
		return "{}", nil
	}

	// There will be at least two curly brackets, N bytes of values,
	// and N-1 bytes of delimiters.
	b := make([]byte, 1, 1+2*n)
	b[0] = '{'
	b = strconv.AppendUint(b, a[0], 10)
	for i := 1; i < n; i++ {
		b = append(b, ',')
		b = strconv.AppendUint(b, a[i], 10)
	}

	return string(append(b, '}')), nil
}

type Uint32Array []uint32

// Scan implements the sql.Scanner interface.
func (a *Uint32Array) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		return a.scanBytes(src)
	case string:
		return a.scanBytes([]byte(src))
	case nil:
		*a = nil
		return nil
	}

	return fmt.Errorf("cannot convert %T to Uint32Array", src)
}

func (a *Uint32Array) scanBytes(src []byte) error {
	elems, err := scanLinearArray(src, []byte{','}, "Uint32Array")
	if err != nil {
		return err
	}

	if *a != nil && len(elems) == 0 {
		*a = (*a)[:0]
	} else {
		b := make(Uint32Array, len(elems))
		for i, v := range elems {
			x, err := strconv.ParseUint(string(v), 10, 32)
			if err != nil {
				return fmt.Errorf("parsing array element index %d: %v", i, err)
			}

			b[i] = uint32(x)
		}

		*a = b
	}

	return nil
}

// Value implements the driver.Valuer interface.
func (a Uint32Array) Value() (driver.Value, error) {
	n := len(a)
	if n == 0 {
		return "{}", nil
	}

	// There will be at least two curly brackets, N bytes of values,
	// and N-1 bytes of delimiters.
	b := make([]byte, 1, 1+2*n)
	b[0] = '{'
	b = strconv.AppendUint(b, uint64(a[0]), 10)
	for i := 1; i < n; i++ {
		b = append(b, ',')
		b = strconv.AppendUint(b, uint64(a[i]), 10)
	}

	return string(append(b, '}')), nil
}

func scanLinearArray(src, del []byte, typ string) (elems [][]byte, err error) {
	dims, elems, err := parseArray(src, del)
	if err != nil {
		return nil, err
	}

	if len(dims) > 1 {
		return nil, fmt.Errorf("pq: cannot convert ARRAY%s to %s",
			strings.Replace(fmt.Sprint(dims), " ", "][", -1), typ)
	}

	return elems, err
}

func parseArray(src, del []byte) (dims []int, elems [][]byte, err error) {
	var depth, i int

	if len(src) < 1 || src[0] != '{' {
		return nil, nil, fmt.Errorf("pq: unable to parse array; expected %q at offset %d", '{', 0)
	}

Open:
	for i < len(src) {
		switch src[i] {
		case '{':
			depth++
			i++
		case '}':
			elems = make([][]byte, 0)
			goto Close
		default:
			break Open
		}
	}

	dims = make([]int, i)

Element:
	for i < len(src) {
		switch src[i] {
		case '{':
			if depth == len(dims) {
				break Element
			}
			depth++
			dims[depth-1] = 0
			i++
		case '"':
			var elem = []byte{}
			var escape bool
			for i++; i < len(src); i++ {
				if escape {
					elem = append(elem, src[i])
					escape = false
				} else {
					switch src[i] {
					default:
						elem = append(elem, src[i])
					case '\\':
						escape = true
					case '"':
						elems = append(elems, elem)
						i++
						break Element
					}
				}
			}
		default:
			for start := i; i < len(src); i++ {
				if bytes.HasPrefix(src[i:], del) || src[i] == '}' {
					elem := src[start:i]
					if len(elem) == 0 {
						return nil, nil, fmt.Errorf("pq: unable to parse array; unexpected %q at offset %d", src[i], i)
					}

					if bytes.Equal(elem, []byte("NULL")) {
						elem = nil
					}

					elems = append(elems, elem)
					break Element
				}
			}
		}
	}

	for i < len(src) {
		if bytes.HasPrefix(src[i:], del) && depth > 0 {
			dims[depth-1]++
			i += len(del)
			goto Element
		} else if src[i] == '}' && depth > 0 {
			dims[depth-1]++
			depth--
			i++
		} else {
			return nil, nil, fmt.Errorf("pq: unable to parse array; unexpected %q at offset %d", src[i], i)
		}
	}

Close:
	for i < len(src) {
		if src[i] == '}' && depth > 0 {
			depth--
			i++
		} else {
			return nil, nil, fmt.Errorf("pq: unable to parse array; unexpected %q at offset %d", src[i], i)
		}
	}

	if depth > 0 {
		err = fmt.Errorf("pq: unable to parse array; expected %q at offset %d", '}', i)
	}

	if err == nil {
		for _, d := range dims {
			if (len(elems) % d) != 0 {
				err = fmt.Errorf("pq: multidimensional arrays must have elements with matching dimensions")
			}
		}
	}

	return
}
