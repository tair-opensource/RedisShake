// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redis

import (
	"reflect"
	"strings"

	"github.com/alibaba/RedisShake/pkg/libs/errors"
	"github.com/alibaba/RedisShake/pkg/libs/log"
)

type HandlerFunc func(arg0 interface{}, args ...[]byte) (Resp, error)

type HandlerTable map[string]HandlerFunc

func NewHandlerTable(o interface{}) (map[string]HandlerFunc, error) {
	if o == nil {
		return nil, errors.Errorf("handler is nil")
	}
	t := make(map[string]HandlerFunc)
	r := reflect.TypeOf(o)
	for i := 0; i < r.NumMethod(); i++ {
		m := r.Method(i)
		if m.Name[0] < 'A' || m.Name[0] > 'Z' {
			continue
		}
		n := strings.ToLower(m.Name)
		if h, err := createHandlerFunc(o, &m.Func); err != nil {
			return nil, err
		} else if _, exists := t[n]; exists {
			return nil, errors.Errorf("func.name = '%s' has already exists", m.Name)
		} else {
			t[n] = h
		}
	}
	return t, nil
}

func MustHandlerTable(o interface{}) map[string]HandlerFunc {
	t, err := NewHandlerTable(o)
	if err != nil {
		log.PanicError(err, "create redis handler map failed")
	}
	return t
}

func createHandlerFunc(o interface{}, f *reflect.Value) (HandlerFunc, error) {
	t := f.Type()
	arg0Type := reflect.TypeOf((*interface{})(nil)).Elem()
	argsType := reflect.TypeOf([][]byte{})
	if t.NumIn() != 3 || t.In(1) != arg0Type || t.In(2) != argsType {
		return nil, errors.Errorf("register with invalid func type = '%s'", t)
	}
	ret0Type := reflect.TypeOf((*Resp)(nil)).Elem()
	ret1Type := reflect.TypeOf((*error)(nil)).Elem()
	if t.NumOut() != 2 || t.Out(0) != ret0Type || t.Out(1) != ret1Type {
		return nil, errors.Errorf("register with invalid func type = '%s'", t)
	}
	return func(arg0 interface{}, args ...[]byte) (Resp, error) {
		var arg0Value reflect.Value
		if arg0 == nil {
			arg0Value = reflect.ValueOf((*interface{})(nil))
		} else {
			arg0Value = reflect.ValueOf(arg0)
		}
		var input, output []reflect.Value
		input = []reflect.Value{reflect.ValueOf(o), arg0Value, reflect.ValueOf(args)}
		if t.IsVariadic() {
			output = f.CallSlice(input)
		} else {
			output = f.Call(input)
		}
		var ret0 Resp
		var ret1 error
		if i := output[0].Interface(); i != nil {
			ret0 = i.(Resp)
		}
		if i := output[1].Interface(); i != nil {
			ret1 = i.(error)
		}
		return ret0, ret1
	}, nil
}

func ParseArgs(resp Resp) (cmd string, args [][]byte, err error) {
	a, err := AsArray(resp, nil)
	if err != nil {
		return "", nil, err
	} else if len(a) == 0 {
		return "", nil, errors.Errorf("empty array")
	}
	bs := make([][]byte, len(a))
	for i := 0; i < len(a); i++ {
		b, err := AsBulkBytes(a[i], nil)
		if err != nil {
			return "", nil, err
		} else {
			bs[i] = b
		}
	}
	cmd = strings.ToLower(string(bs[0]))
	if cmd == "" {
		return "", nil, errors.Errorf("empty command")
	}
	return cmd, bs[1:], nil
}

func ChangeArgsToResp(cmd []byte, args [][]byte) (resp Resp) {
	array := make([]Resp, len(args)+1)
	array[0] = &BulkBytes{cmd}
	for i := 0; i < len(args); i++ {
		array[i+1] = &BulkBytes{args[i]}
	}
	return &Array{array}
}
