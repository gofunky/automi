package unary

import (
	"context"
	"fmt"
	"github.com/emirpasic/gods/containers"
	"github.com/gofunky/pyraset/v2"
	"reflect"

	"github.com/gofunky/automi/api"
)

// ProcessFunc returns a unary function which applies the specified
// user-defined function that processes data items from upstream and
// returns a result value. The provided function must be of type:
//   func(T) (R, error)
//   where T is the type of incoming item
//   R the type of returned processed item
func ProcessFunc(f interface{}) (api.UnFunc, error) {
	fntype := reflect.TypeOf(f)
	if err := isUnaryFuncForm(fntype); err != nil {
		return nil, err
	}

	fnval := reflect.ValueOf(f)

	return api.UnFunc(func(ctx context.Context, data interface{}) (result interface{}, err error) {
		arg0 := reflect.ValueOf(data)
		call := fnval.Call([]reflect.Value{arg0})
		result = call[0].Interface()
		if len(call) > 1 && !call[1].IsNil() {
			err = call[1].Interface().(error)
		}
		return
	}), nil
}

// FilterFunc returns a unary function (api.UnFunc) which applies the user-defined
// filtering to apply predicates that filters out data items from being included
// in the downstream.  The provided user-defined function must be of type:
//   func(T) (bool, error) - where T is the type of incoming data item, bool is the value of the predicate
// When the user-defined function returns false, the current processed data item will not
// be placed in the downstream processing.
func FilterFunc(f interface{}) (api.UnFunc, error) {
	fntype := reflect.TypeOf(f)
	if err := isUnaryFuncForm(fntype); err != nil {
		return nil, err
	}
	// ensure bool ret type
	if fntype.Out(0).Kind() != reflect.Bool {
		panic("Filter function must return a bool type")
	}

	fnval := reflect.ValueOf(f)

	return api.UnFunc(func(ctx context.Context, data interface{}) (interface{}, error) {
		arg0 := reflect.ValueOf(data)
		call := fnval.Call([]reflect.Value{arg0})
		if len(call) > 1 {
			if err := call[1].Interface().(error); err != nil {
				return nil, err
			}
		}
		if predicate := call[0].Bool(); !predicate {
			return nil, nil
		}
		return data, nil
	}), nil
}

// MapFunc returns an unary function which applies the user-defined function which
// maps, one-to-one, the incomfing value to a new value.  The user-defined function
// must be of type:
//   func(T) (R, error) - where T is the incoming item, R is the type of the returned mapped item
func MapFunc(f interface{}) (api.UnFunc, error) {
	return ProcessFunc(f)
}

// FlatMapFunc returns an unary function which applies a user-defined function which
// takes incoming comsite items and deconstruct them into individual items which can
// then be re-streamed.  The type for the user-defined function is:
//   func (T) (R, error) - where R is the original item, R is a slice of decostructed items
// The slice returned should be restreamed by placing each item onto the stream for
// downstream processing.
// Besides slices, arrays, maps, and sets are also accepted.
func FlatMapFunc(f interface{}) (api.UnFunc, error) {
	fntype := reflect.TypeOf(f)
	if err := isUnaryFuncForm(fntype); err != nil {
		return nil, err
	}
	switch fntype.Out(0).Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		// Do nothing
	default:
		retType := fntype.Out(0)
		mapSetType := reflect.TypeOf((*mapset.Set)(nil)).Elem()
		containerType := reflect.TypeOf((*containers.Container)(nil)).Elem()
		if !retType.Implements(mapSetType) && !retType.Implements(containerType) {
			return nil, fmt.Errorf("FlatMap function must return a slice, array, map, mapset, or container, actual type %v", fntype.Out(0).Name())
		}
	}

	fnval := reflect.ValueOf(f)

	return api.UnFunc(func(ctx context.Context, data interface{}) (result interface{}, err error) {
		arg0 := reflect.ValueOf(data)
		call := fnval.Call([]reflect.Value{arg0})
		result = call[0].Interface()
		if len(call) > 1 && !call[1].IsNil() {
			err = call[1].Interface().(error)
		}
		return
	}), nil
}

// isUnaryFuncForm ensures type is a function of form func(in)out or func(in)(out, error).
func isUnaryFuncForm(ftype reflect.Type) error {
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	switch ftype.Kind() {
	case reflect.Func:
		if ftype.NumIn() != 1 {
			return fmt.Errorf("unary func %v must take one parameter", ftype.String())
		}
		if ftype.NumOut() == 0 || ftype.NumOut() > 2 {
			return fmt.Errorf("unary func %v must return one value or two with the second being an error",
				ftype.String())
		} else if ftype.NumOut() == 2 && !ftype.Out(1).Implements(errorInterface) {
			return fmt.Errorf("the second return value's type of the unary func %v must be an error", ftype.String())
		}
	default:
		return fmt.Errorf("unary func %v of type func(T)R or func(T)(R, error)", ftype.String())
	}
	return nil
}
