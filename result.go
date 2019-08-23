package queue

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/three-plus-three/modules/as"
	"github.com/three-plus-three/modules/util"
)

type result struct {
	value interface{}
	err   error
}

type asyncResult struct {
	task *Task
	srv  *Server
	c    chan *result
}

func (result *asyncResult) ID() TaskID {
	return result.task.ID
}
func (result *asyncResult) State() (TaskStatus, error) {
	return result.srv.backend.GetState()
}

func (result *asyncResult) Cancel() (bool, error) {
	return false, errors.New("notimplemented")
}
func (result *asyncResult) Get(ctx context.Context, value interface{}) error {
	r := <-result.c
	if r.err != nil {
		return r.err
	}

	return assignValue(value, r.value)
}

func assignValue(recv, from interface{}) (err error) {
	switch value := recv.(type) {
	case *string:
		*value, err = as.String(from)
	case *int:
		*value, err = as.Int(from)
	case *int64:
		*value, err = as.Int64(from)
	case *uint:
		*value, err = as.Uint(from)
	case *uint64:
		*value, err = as.Uint64(from)
	case *int32:
		*value, err = as.Int32(from)
	case *uint32:
		*value, err = as.Uint32(from)
	case *int16:
		*value, err = as.Int16(from)
	case *uint16:
		*value, err = as.Uint16(from)
	case *int8:
		*value, err = as.Int8(from)
	case *uint8:
		*value, err = as.Uint8(from)
	case *float32:
		*value, err = as.Float32(from)
	case *float64:
		*value, err = as.Float64(from)
	case *map[string]interface{}:
		if values, ok := from.(map[string]interface{}); ok {
			if value == nil {
				*value = values
			} else {
				for key, value := range values {
					*value[key] = value
				}
			}
		} else {
			err = fmt.Errorf("result isnot excepted type - %T", from)
		}
	case *map[string]string:
		if values, ok := from.(map[string]string); ok {
			*value = *values
		} else if values, ok := from.(map[string]interface{}); ok {
			if value == nil {
				*value = map[string]string{}
			}
			for key, value := range values {
				*value[key] = as.StringWithDefault(value, "")
			}
		} else {
			err = fmt.Errorf("result isnot excepted type - %T", from)
		}
	case *[]string:
		*value, err = as.Strings(from)
	case *[]int:
		*value, err = as.Ints(from)
	case *[]int64:
		*value, err = as.Int64s(from)
	case *[]uint:
		*value, err = as.Uints(from)
	case *[]uint64:
		*value, err = as.Uint64s(from)
	case *[]byte:
		*value, err = as.Bytes(from)
	default:
		rv := reflect.ValueOf(recv)
		if rv.Kind() != reflect.Ptr {
			err = errors.New("argument 'recv' must is a pointer")
			return
		} else {
			rv = rv.Elem()
		}

		if rv.Kind() == reflect.Struct {
			values, ok := from.(map[string]interface{})
			if !ok {
				err = fmt.Errorf("result isnot excepted type - %T", from)
				return
			}
			return util.ToStruct(recv, from)
		}
		return fmt.Errorf("argument 'recv' isnot excepted type -%T", recv)
	}
}
