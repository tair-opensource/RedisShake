// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package fmt2

import (
	"math"
	"reflect"
	"strconv"

	"pkg/libs/errors"
)

func Num64(i interface{}) (n64 interface{}, ok bool) {
	switch x := i.(type) {
	case int:
		n64 = int64(x)
	case int8:
		n64 = int64(x)
	case int16:
		n64 = int64(x)
	case int32:
		n64 = int64(x)
	case int64:
		n64 = int64(x)
	case uint:
		n64 = uint64(x)
	case uint8:
		n64 = uint64(x)
	case uint16:
		n64 = uint64(x)
	case uint32:
		n64 = uint64(x)
	case uint64:
		n64 = uint64(x)
	case float32:
		n64 = float64(x)
	case float64:
		n64 = float64(x)
	default:
		return i, false
	}
	return n64, true
}

func ParseFloat64(i interface{}) (float64, error) {
	if v, ok := Num64(i); ok {
		switch x := v.(type) {
		case int64:
			return float64(x), nil
		case uint64:
			return float64(x), nil
		case float64:
			switch {
			case math.IsNaN(x):
				return 0, errors.Errorf("parse nan float64")
			case math.IsInf(x, 0):
				return 0, errors.Errorf("parse inf float64")
			}
			return float64(x), nil
		default:
			return 0, errors.Errorf("parse float64 from unknown num64")
		}
	} else {
		var s string
		switch x := i.(type) {
		case nil:
			return 0, errors.Errorf("parse float64 from nil")
		case string:
			s = x
		case []byte:
			s = string(x)
		default:
			return 0, errors.Errorf("parse float64 from <%s>", reflect.TypeOf(i))
		}
		f, err := strconv.ParseFloat(s, 64)
		return f, errors.Trace(err)
	}
}

func ParseInt64(i interface{}) (int64, error) {
	if v, ok := Num64(i); ok {
		switch x := v.(type) {
		case int64:
			return int64(x), nil
		case uint64:
			if x > math.MaxInt64 {
				return 0, errors.Errorf("parse int64 from uint64, overflow")
			}
			return int64(x), nil
		case float64:
			switch {
			case math.IsNaN(x):
				return 0, errors.Errorf("parse int64 from nan float64")
			case math.IsInf(x, 0):
				return 0, errors.Errorf("parse int64 from inf float64")
			case math.Abs(x-float64(int64(x))) > 1e-9:
				return 0, errors.Errorf("parse int64 from inv float64")
			}
			return int64(x), nil
		default:
			return 0, errors.Errorf("parse int64 from unknown num64")
		}
	} else {
		var s string
		switch x := i.(type) {
		case nil:
			return 0, errors.Errorf("parse int64 from nil")
		case string:
			s = x
		case []byte:
			s = string(x)
		default:
			return 0, errors.Errorf("parse int64 from <%s>", reflect.TypeOf(i))
		}
		v, err := strconv.ParseInt(s, 10, 64)
		return v, errors.Trace(err)
	}
}

func ParseUint64(i interface{}) (uint64, error) {
	if v, ok := Num64(i); ok {
		switch x := v.(type) {
		case int64:
			if x < 0 {
				return 0, errors.Errorf("parse uint64 from int64, overflow")
			}
			return uint64(x), nil
		case uint64:
			return uint64(x), nil
		case float64:
			switch {
			case math.IsNaN(x):
				return 0, errors.Errorf("parse uint64 from nan float64")
			case math.IsInf(x, 0):
				return 0, errors.Errorf("parse uint64 from inf float64")
			case math.Abs(x-float64(uint64(x))) > 1e-9:
				return 0, errors.Errorf("parse uint64 from inv float64")
			}
			return uint64(x), nil
		default:
			return 0, errors.Errorf("parse int64 from unknown num64")
		}
	} else {
		var s string
		switch x := i.(type) {
		case nil:
			return 0, errors.Errorf("parse uint64 from nil")
		case string:
			s = x
		case []byte:
			s = string(x)
		default:
			return 0, errors.Errorf("parse uint64 from <%s>", reflect.TypeOf(i))
		}
		v, err := strconv.ParseUint(s, 10, 64)
		return v, errors.Trace(err)
	}
}
