package sqlkungfu

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// http://godoc.org/github.com/jmoiron/sqlx

const (
	mapKey  = "sqlmapkey"
	mapKey2 = "sqlmapkey2"
)

// map
// normal
// join
// To-Do:
// - do not override non-nil values
func Unmarshal(rows *sql.Rows, v interface{}) (err error) {
	vv := reflect.ValueOf(v)
	if vv.Kind() != reflect.Ptr {
		return fmt.Errorf("sqlkungfu: Unmarshal(non-pointer %T)", v)
	}

	vv = indirect(vv)
	columns, err := rows.Columns()
	if err != nil {
		return
	}
	if vv.Kind() == reflect.Map && vv.IsNil() {
		vv.Set(reflect.MakeMap(vv.Type()))
	}
	switch vv.Kind() {
	case reflect.Struct:
		rows.Next()
		if err = unmarshalStruct(rows, columns, vv); err != nil {
			return
		}
		if err = rows.Close(); err != nil {
			return
		}
	case reflect.Slice:
		// TODO:
		// - []string
		// - [][]string
		// - []struct{}
		// - []map[string]struct{} ?
		// - []map[string]string{}
		// - []map[string][]string{} ?
		vet := vv.Type().Elem()
		for rows.Next() {
			e := newValue(vet)
			ee := indirect(e)
			switch ee.Kind() {
			case reflect.Struct:
				if err = unmarshalStruct(rows, columns, ee); err != nil {
					return
				}
				vv = reflect.Append(vv, e.Elem())
			case reflect.Map:
				keyt := vet.Key()
				valt := vet.Elem()
				fields, key, val := genMapFields(columns, keyt, valt)
				if err = rows.Scan(fields...); err != nil {
					return
				}

				ee.Set(reflect.MakeMap(ee.Type()))
				switch indirectT(valt).Kind() {
				case reflect.Slice:
					v := reflect.Append(indirect(newValue(valt)), val.([]reflect.Value)...)
					ee.SetMapIndex(key.Elem(), v)
				case reflect.Array:
					// TODO: add tests
					v := newValue(valt).Elem()
					ve := indirect(v)
					for i, e := range val.([]reflect.Value) {
						ve.Index(i).Set(e)
					}
					vv.SetMapIndex(key.Elem(), v)
				case reflect.Map:
					// TODO
				default:
					ee.SetMapIndex(key.Elem(), val.(reflect.Value).Elem())
				}
				vv = reflect.Append(vv, e.Elem())
			case reflect.Slice:
				var eis []reflect.Value
				var slice []interface{}
				l := len(columns)
				eet := ee.Type()
				for i := 0; i < l; i++ {
					ei := newValue(eet.Elem()).Elem()
					eis = append(eis, ei)
					slice = append(slice, ei.Addr().Interface())
				}
				if err = rows.Scan(slice...); err != nil {
					return
				}
				ee.Set(reflect.Append(ee, eis...))
				vv = reflect.Append(vv, e.Elem())
			case reflect.Array:
			default:
				if err = rows.Scan(e.Interface()); err != nil {
					return
				}
				vv = reflect.Append(vv, e.Elem())
			}
		}
		reflect.ValueOf(v).Elem().Set(vv)
	case reflect.Array:
		// TODO:
		// - []string
		// - [][]string
		// - []struct{}
		// - []map[string]string{}
		var i int
		vet := vv.Type().Elem()
		for rows.Next() {
			switch vet.Kind() {
			case reflect.Struct, reflect.Ptr:
				elem := newValue(vet)
				if err = unmarshalStruct(rows, columns, indirect(elem)); err != nil {
					return
				}
				vv.Index(i).Set(elem.Elem())
				i++
			case reflect.Map:
				// TODO
			case reflect.Slice:
			case reflect.Array:
			// case reflect.Ptr:

			default:
			}
		}
	case reflect.Map:
		// TODO:
		// - map[string]string
		// - map[*string]string
		// - map[string][]string
		// - map[string][][]string
		// - map[string]struct{}
		// - map[string]map[string]string{}
		// - map[string][]struct{}
		// - map[string][]map[string]string{}
		// - map[struct]struct{}
		vvt := vv.Type()
		keyt := vvt.Key()
		valt := vvt.Elem()
		for rows.Next() {
			fields, key, val := genMapFields(columns, keyt, valt)

			if err = rows.Scan(fields...); err != nil {
				return
			}

			switch valet := indirectT(valt); valet.Kind() {
			case reflect.Slice:
				v := vv.MapIndex(key.Elem())
				if !v.IsValid() {
					v = newValue(valt).Elem()
				}
				switch indirectT(valet.Elem()).Kind() {
				case reflect.Struct:
					if v.Kind() == reflect.Ptr {
						ve := indirect(v)
						ve.Set(reflect.Append(ve, val.(reflect.Value).Elem()))
					} else {
						v = reflect.Append(v, val.(reflect.Value).Elem())
					}
				case reflect.Slice:
					// TODO: test
					vale := newValue(valet.Elem()).Elem()
					if vale.Kind() == reflect.Ptr {
						valee := indirect(vale)
						valee.Set(reflect.Append(valee, val.([]reflect.Value)...))
					} else {
						vale = reflect.Append(vale, val.([]reflect.Value)...)
					}
					if v.Kind() == reflect.Ptr {
						ve := indirect(v)
						ve.Set(reflect.Append(ve, vale))
					} else {
						v = reflect.Append(v, vale)
					}
				case reflect.Array:
					// TODO
				case reflect.Map:
					vals := val.([]reflect.Value)
					vale := newValue(valet.Elem()).Elem()
					valee := vale
					if vale.Kind() == reflect.Ptr {
						valee = indirect(vale)
					}
					for i, col := range columns {
						if col == mapKey {
							continue
						}
						valee.SetMapIndex(reflect.ValueOf(col), vals[i])
					}
					if v.Kind() == reflect.Ptr {
						ve := indirect(v)
						ve.Set(reflect.Append(ve, vale))
					} else {
						v = reflect.Append(v, vale)
					}
				default:
					v = newValue(valt).Elem()
					if v.Kind() == reflect.Ptr {
						ve := indirect(v)
						ve.Set(reflect.Append(ve, val.([]reflect.Value)...))
					} else {
						v = reflect.Append(v, val.([]reflect.Value)...)
					}
				}
				vv.SetMapIndex(key.Elem(), v)
			case reflect.Array:
				v := newValue(valt).Elem()
				ve := v
				if v.Kind() == reflect.Ptr {
					ve = indirect(v)
				}
				for i, e := range val.([]reflect.Value) {
					ve.Index(i).Set(e)
				}
				vv.SetMapIndex(key.Elem(), v)
			case reflect.Map:
				vals := val.([]reflect.Value)
				v := newValue(valt).Elem()
				ve := v
				if v.Kind() == reflect.Ptr {
					ve = indirect(v)
				}
				for i, col := range columns {
					if col == mapKey {
						continue
					}
					ve.SetMapIndex(reflect.ValueOf(col), vals[i])
				}
				vv.SetMapIndex(key.Elem(), v)
			default:
				vv.SetMapIndex(key.Elem(), val.(reflect.Value).Elem())
			}
		}
		reflect.ValueOf(v).Elem().Set(vv)
	default:
	}

	err = rows.Err()
	return
}

func unmarshalStruct(rows *sql.Rows, columns []string, v reflect.Value) (err error) {
	if err = rows.Scan(getFields(v, columns)...); err != nil {
		return
	}

	return
}

func getFields(v reflect.Value, columns []string) (fields []interface{}) {
	// TODO: imporve?
	for _, col := range columns {
		fields = append(fields, getField(v, col).Addr().Interface())
	}
	return
}

// support multiple name casting: snake, lowercases
func getField(v reflect.Value, name string) (f reflect.Value) {
	// TODO: indirect(v) here is overkill?
	f = indirect(v).FieldByNameFunc(func(field string) bool {
		return strings.ToLower(field) == name
	})
	if f.Kind() != reflect.Invalid {
		return
	}

	// handle nested fields: level1.level2.field
	return
}

func indirect(v reflect.Value) reflect.Value {
	if v.Kind() != reflect.Ptr {
		return v
	}

	for {
		v = v.Elem()
		if v.Kind() != reflect.Ptr {
			break
		}

		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
	}

	return v
}

func indirectT(v reflect.Type) reflect.Type {
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	return v
}

// TODO: refactor better naming?
func genMapFields(columns []string, keyt, valt reflect.Type) (fields []interface{}, key reflect.Value, val interface{}) {
	key = newValue(keyt)
	valet := indirectT(valt)
	if k := valet.Kind(); k == reflect.Array || k == reflect.Slice {
		switch nvalet := indirectT(valet.Elem()); nvalet.Kind() {
		case reflect.Struct, reflect.Slice, reflect.Array, reflect.Map:
			valt = valet.Elem()
			valet = nvalet
		}
	}

	for i, col := range columns {
		if col == mapKey {
			fields = append(fields, indirect(key).Addr().Interface())
			continue
		}

		switch valet.Kind() {
		case reflect.Struct:
			if val == nil {
				val = newValue(valt)
			}
			fields = append(fields, getField(val.(reflect.Value), col).Addr().Interface())
		case reflect.Slice:
			if val == nil {
				// TODO: why can't we use a reflect.Slice
				val = []reflect.Value{}
			}
			e := newValue(valet.Elem()).Elem()
			ee := e
			if e.Kind() == reflect.Ptr {
				ee = indirect(e)
			}
			val = append(val.([]reflect.Value), e)
			fields = append(fields, ee.Addr().Interface())
		case reflect.Array:
			if val == nil {
				val = []reflect.Value{}
			}
			e := newValue(valet.Elem()).Elem()
			ee := e
			if e.Kind() == reflect.Ptr {
				ee = indirect(e)
			}
			val = append(val.([]reflect.Value), e)
			fields = append(fields, ee.Addr().Interface())
		case reflect.Map:
			if val == nil {
				val = make([]reflect.Value, len(columns))
			}
			e := newValue(valet.Elem()).Elem()
			ee := e
			if e.Kind() == reflect.Ptr {
				ee = indirect(e)
			}
			// indirect(val.(reflect.Value)).SetMapIndex(reflect.ValueOf(col), e)
			// val = append(val.([]reflect.Value), e)
			val.([]reflect.Value)[i] = e
			fields = append(fields, ee.Addr().Interface())
		default:
			v := newValue(valt)
			ve := v
			if v.Kind() == reflect.Ptr {
				ve = indirect(v)
			}
			fields = append(fields, ve.Addr().Interface())
			val = v
		}
	}

	return
}

// // TODO: should not override exist value
// func initValue(v reflect.Value) reflect.Value {
// 	// for i, item := range itmes {

// 	// }
// 	return v
// }

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

	if e := v.Elem(); e.Kind() == reflect.Map && e.IsNil() {
		v.Elem().Set(reflect.MakeMap(v.Elem().Type()))
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
