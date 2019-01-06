package binary

import (
	"context"
	"fmt"
	"reflect"

	"github.com/gofunky/automi/api"
)

// ReduceFunc returns a binary function which takes a user-defined accumulator
// function to apply reduction (fold) logic to incoming streaming items to
// return a single summary value.  The user-provided accumulator function must
// be of type:
//   func(S,T) R
//     where S is the partial result (initially the seed)
//     T is the streamed item from upstream
//     R is the calculated value which becomes partial result for next value
// It is important to understand that applying a reductive operator after an
// open-ended emitter (i.e. a network) may never end.  To force a Reduction function
// to terminate, it is sensible to place it after a batch operator for instance.
func ReduceFunc(f interface{}) (api.BinFunc, error) {
	fntype := reflect.TypeOf(f)
	if err := isBinaryFuncForm(fntype); err != nil {
		return nil, err
	}

	fnval := reflect.ValueOf(f)

	return api.BinFunc(func(ctx context.Context, op0, op1 interface{}) (result interface{}, err error) {
		arg0 := reflect.ValueOf(op0)
		arg1, arg1Type := reflect.ValueOf(op1), reflect.TypeOf(op1)
		if op0 == nil {
			arg0 = reflect.Zero(arg1Type)
		}
		call := fnval.Call([]reflect.Value{arg0, arg1})
		result = call[0].Interface()
		if len(call) > 1 && !call[1].IsNil() {
			err = call[1].Interface().(error)
		}
		return
	}), nil

}

func isBinaryFuncForm(ftype reflect.Type) error {
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	switch ftype.Kind() {
	case reflect.Func:
		if ftype.NumIn() != 2 {
			return fmt.Errorf("binary func %v must take two parameter", ftype.String())
		}
		if ftype.NumOut() == 0 || ftype.NumOut() > 2 {
			return fmt.Errorf("binary func %v must return one value or two with the second being an error",
				ftype.String())
		} else if ftype.NumOut() == 2 && !ftype.Out(1).Implements(errorInterface) {
			return fmt.Errorf("the second return value's type of the binary func %v must be an error", ftype.String())
		}
	default:
		return fmt.Errorf("binary func %v must be of type func(S,T)R or func(S,T)(R, error)", ftype.String())
	}
	return nil
}
