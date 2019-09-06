package queue

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/runner-mei/errors"
	"github.com/three-plus-three/modules/as"
	"github.com/three-plus-three/modules/util"
)

type asyncResult struct {
	taskID TaskID
	srv    *Server
	c      chan *asyncResult

	value interface{}
	err   error
}

func (result *asyncResult) ID() TaskID {
	return result.taskID
}
func (result *asyncResult) State() (TaskStatus, error) {
	state, err := result.srv.backend.GetState(context.Background(), result.taskID)
	if err != nil {
		return TaskStatusInit, err
	}
	return state.State, nil
}
func (result *asyncResult) Cancel() error {
	err := result.srv.cancel(result.taskID)
	result.reply(nil, context.Canceled)
	return err
}
func (result *asyncResult) Get(ctx context.Context, value interface{}) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			state, err := result.srv.backend.GetState(ctx, result.taskID)
			if err != nil {
				return err
			}
			if state.IsCompleted() {
				if state.State == TaskStatusFailure {
					if state.Error == nil {
						state.Error = errors.New("错误!!!")
					}
					return state.Error
				}

				return assignValue(value, state.Result)
			}
		case <-ctx.Done():
			return ctx.Err()
		case r := <-result.c:
			if r.err != nil {
				return r.err
			}

			return assignValue(value, r.value)
		}
	}
}
func (result *asyncResult) reply(value interface{}, err error) bool {
	result.value = value
	result.err = err
	select {
	case result.c <- result:
		return true
	default:
		return false
	}
}

func assignValue(recv, from interface{}) (err error) {
	if from == nil {
		return nil
	}
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
