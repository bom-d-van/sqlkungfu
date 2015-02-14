package sqlkungfu

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// http://godoc.org/github.com/jmoiron/sqlx

// map
// normal
// join
func Unmarshal(rows *sql.Rows, v interface{}) (err error) {
	vv := reflect.ValueOf(v)
	vk := vv.Kind()
	if vk != reflect.Ptr {
		return fmt.Errorf("sqlkungfu: Unmarshal(non-pointer %T)", v)
	}

	vv = indirect(vv)
	vk = vv.Kind()
	columns, err := rows.Columns()
	if err != nil {
		return
	}
	switch vk {
	case reflect.Struct:
		rows.Next()
		if err = unmarshalStruct(rows, columns, vv); err != nil {
			return
		}
		rows.Close()
	case reflect.Slice:
		for rows.Next() {
			elem := newValue(vv.Type().Elem())
			if err = unmarshalStruct(rows, columns, indirect(elem)); err != nil {
				return
			}
			vv = reflect.Append(vv, elem.Elem())
		}
		reflect.ValueOf(v).Elem().Set(vv)
	case reflect.Array:
		//
	}

	// var record reflect.Value

	err = rows.Err()
	return
}

func unmarshalStruct(rows *sql.Rows, columns []string, v reflect.Value) (err error) {
	var fields []interface{}
	// TODO: imporve?
	for _, col := range columns {
		fields = append(fields, getField(v, col).Addr().Interface())
	}

	if err = rows.Scan(fields...); err != nil {
		return
	}

	return
}

func indirect(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	return v
}

// 1
// 	t **
// 	v ***
// 	v **
// 	t *
// 	e **
// 2
// 	t #
// 	e *
// 	v *
func newValue(t reflect.Type) (v reflect.Value) {
	v = reflect.New(t)
	ov := v
	for t.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
		e := reflect.New(t)
		v.Set(e)
	}
	return ov
}

// func newValueRecursive(t reflect.Type) (v reflect.Value) {
// 	v = reflect.New(t)
// 	if t.Kind() == reflect.Ptr {
// 		v.Elem().Set(newValueRecursive(t.Elem()))
// 	}
// 	return
// }

// support multiple name casting: snake, lowercases
func getField(v reflect.Value, name string) reflect.Value {
	return reflect.Indirect(v).FieldByNameFunc(func(field string) bool {
		return strings.ToLower(field) == name
	})
}

func UnmarshalRow(row *sql.Row, v interface{}) error {
	return nil
}

func Insert(db *sql.DB, v interface{}) (string, error) {
	return "", nil
}

func Update(db *sql.DB, v interface{}) (string, error) {
	return "", nil
}

func Columns(v interface{}) (cols []string) {
	vt := reflect.TypeOf(v)
	count := vt.NumField()
	for i := 0; i < count; i++ {
		field := vt.Field(i)
		cols = append(cols, strings.ToLower(field.Name))
	}

	return nil
}

func ColumnsPrefix(v interface{}, prefix string) []string {
	return nil
}
