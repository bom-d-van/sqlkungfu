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
// - convert []uint8 into a string for interface{} values
func Unmarshal(rows *sql.Rows, v interface{}) (err error) {
	defer func() {
		if er := rows.Close(); er != nil {
			if err == nil {
				err = er
			}
			return
		}

		if err == nil {
			err = rows.Err()
		}
	}()
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
					ee.SetMapIndex(key, v)
				case reflect.Array:
					// TODO: add tests
					v := newValue(valt).Elem()
					ve := indirect(v)
					for i, e := range val.([]reflect.Value) {
						ve.Index(i).Set(e)
					}
					vv.SetMapIndex(key, v)
				case reflect.Map:
					// TODO
				default:
					ee.SetMapIndex(key, val.(reflect.Value).Elem())
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
		//
		// - map[struct]struct{}
		//
		// - map[string][]string
		// - map[string]struct{}
		// - map[string]map[string]string{}
		//
		// - map[string][][]string
		// - map[string][]struct{}
		// - map[string][]map[string]string{}
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
				v := vv.MapIndex(key)
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
					vale := newValue(valet.Elem()).Elem()
					if vale.Kind() == reflect.Ptr {
						valee := indirect(vale)
						for i, e := range val.([]reflect.Value) {
							valee.Index(i).Set(e)
						}
					} else {
						for i, e := range val.([]reflect.Value) {
							vale.Index(i).Set(e)
						}
					}
					if v.Kind() == reflect.Ptr {
						ve := indirect(v)
						ve.Set(reflect.Append(ve, vale))
					} else {
						v = reflect.Append(v, vale)
					}
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
				vv.SetMapIndex(key, v)
			case reflect.Array:
				// TODO: same support as slice above
				v := newValue(valt).Elem()
				ve := v
				if v.Kind() == reflect.Ptr {
					ve = indirect(v)
				}
				for i, e := range val.([]reflect.Value) {
					ve.Index(i).Set(e)
				}
				vv.SetMapIndex(key, v)
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
				vv.SetMapIndex(key, v)
			case reflect.Interface:
				fallthrough
			default:
				vv.SetMapIndex(key, val.(reflect.Value).Elem())
			}
		}
		reflect.ValueOf(v).Elem().Set(vv)
	default:
		return fmt.Errorf("sqlkungfu: Unmarshal(unsupported type %T)", v)
	}

	return
}

func unmarshalStruct(rows *sql.Rows, columns []string, v reflect.Value) (err error) {
	var (
		fields   []interface{}
		valMap   = map[string]reflect.Value{}
		fieldMap = map[string]struct {
			reflect.Value
			valMapPrefix string
			names        []string
		}{}
	)
	for _, col := range columns {
		field, pos := getField(v, col)
		if !field.IsValid() {
			continue
		}
		field = indirect(field)
		var val reflect.Value
		switch field.Kind() {
		case reflect.Slice, reflect.Array:
			info := fieldMap[col]
			info.Value = field
			val = newValue(field.Type().Elem())
			fieldMap[col] = info
		case reflect.Map:
			if field.IsNil() {
				field.Set(newValue(field.Type()).Elem())
			}
			info := fieldMap[col]
			names := strings.Split(col, ".")
			info.names = names[pos:]
			info.Value = field
			index := names[pos-1]
			info.valMapPrefix = index
			val = info.Value
			if val, info.names, err = parseColSchema(valMap, val, index, info.names); err != nil {
				return
			}
			// var tlen int
			// val = info.Value
			// for i, name := range info.names {
			// 	if index != "" {
			// 		index += "."
			// 	}
			// 	index += name
			// 	vale := indirect(val)
			// 	switch vale.Kind() {
			// 	case reflect.Struct:
			// 		val = vale.FieldByNameFunc(func(f string) bool {
			// 			return strings.ToLower(f) == name
			// 		})
			// 		if !val.IsValid() {
			// 			return fmt.Errorf("can't find %s in %s", name, vale.Type())
			// 		}
			// 		val = val.Addr()
			// 		valMap[index] = val
			// 		tlen++
			// 	case reflect.Map:
			// 		var ok bool
			// 		if val, ok = valMap[index]; !ok {
			// 			et := vale.Type().Elem()
			// 			// TODO:
			// 			// - what's the better way to check if it's a interface{}
			// 			// - should use indirect here?
			// 			if et.Kind() == reflect.Interface && et.NumMethod() == 0 && i < len(info.names)-1 {
			// 				et = reflect.TypeOf(map[string]interface{}{})
			// 			} else {
			// 				switch indirectT(et).Kind() {
			// 				case reflect.Interface, reflect.Map, reflect.Struct:
			// 				default:
			// 					index += "." + strings.Join(info.names[i+1:], ".")
			// 				}
			// 			}
			// 			val = newValue(et)
			// 			valMap[index] = val
			// 		}
			// 		tlen++
			// 	}
			// }
			// names = append(info.names[:tlen-1], strings.Join(info.names[tlen-1:], "."))
			// info.names = names
			fieldMap[col] = info
		default:
			val = field.Addr()
		}

		fields = append(fields, val.Interface())
	}

	if err = rows.Scan(fields...); err != nil {
		return
	}

	var arri int
	for i, col := range columns {
		field := fieldMap[col]
		switch field.Kind() {
		case reflect.Slice:
			field.Set(reflect.Append(field.Value, reflect.ValueOf(fields[i]).Elem()))
		case reflect.Array:
			field.Index(arri).Set(reflect.ValueOf(fields[i]).Elem())
			arri++
		case reflect.Map:
			if field.IsNil() {
				field.Set(newValue(field.Type()).Elem())
			}

			// var (
			// 	l   = len(field.names)
			// 	val = reflect.ValueOf(fields[i])
			// )
			// // TODO: be configurable (maybe by struct tags?)
			// if b, ok := val.Elem().Interface().([]uint8); ok {
			// 	s := string(b)
			// 	val = reflect.ValueOf(&s)
			// }
			// for i, _ := range field.names[1:] {
			// 	name := field.names[l-i-1]
			// 	v, _ := valMap[field.valMapPrefix+"."+strings.Join(field.names[:l-i-1], ".")]
			// 	ve := indirect(v)
			// 	switch ve.Kind() {
			// 	case reflect.Struct:
			// 		ve.FieldByNameFunc(func(f string) bool {
			// 			return strings.ToLower(f) == name
			// 		}).Set(val.Elem())
			// 	case reflect.Map:
			// 		ve.SetMapIndex(reflect.ValueOf(name), val.Elem())
			// 	}
			// 	valMap[field.valMapPrefix+"."+strings.Join(field.names[:l-i-1], ".")] = v
			// 	val = v
			// }
			// field.SetMapIndex(reflect.ValueOf(field.names[0]), val.Elem())
			setColSchema(field.names, field.Value, reflect.ValueOf(fields[i]), valMap, field.valMapPrefix)
		}
	}

	return
}

func parseColSchema(valMap map[string]reflect.Value, val reflect.Value, index string, names []string) (reflect.Value, []string, error) {
	var tlen int
	for i, name := range names {
		if index != "" {
			index += "."
		}
		index += name
		vale := indirect(val)
		switch vale.Kind() {
		case reflect.Struct:
			val = vale.FieldByNameFunc(func(f string) bool {
				return strings.ToLower(f) == name
			})
			if !val.IsValid() {
				return val, names, fmt.Errorf("can't find %s in %s", name, vale.Type())
			}
			val = val.Addr()
			valMap[index] = val
			tlen++
		case reflect.Map:
			var ok bool
			if val, ok = valMap[index]; !ok {
				et := vale.Type().Elem()
				// TODO:
				// - what's the better way to check if it's a interface{}
				// - should use indirect here?
				if et.Kind() == reflect.Interface && et.NumMethod() == 0 && i < len(names)-1 {
					et = reflect.TypeOf(map[string]interface{}{})
				} else {
					switch indirectT(et).Kind() {
					case reflect.Interface, reflect.Map, reflect.Struct:
					default:
						index += "." + strings.Join(names[i+1:], ".")
					}
				}
				val = newValue(et)
				valMap[index] = val
			}
			tlen++
		}
	}
	names = append(names[:tlen-1], strings.Join(names[tlen-1:], "."))
	return val, names, nil
}

func setColSchema(names []string, field, val reflect.Value, valMap map[string]reflect.Value, valMapPrefix string) {
	l := len(names)
	// TODO: be configurable (maybe by struct tags?)
	if b, ok := val.Elem().Interface().([]uint8); ok {
		s := string(b)
		val = reflect.ValueOf(&s)
	}
	for i, _ := range names[1:] {
		name := names[l-i-1]
		v, _ := valMap[valMapPrefix+"."+strings.Join(names[:l-i-1], ".")]
		ve := indirect(v)
		switch ve.Kind() {
		case reflect.Struct:
			ve.FieldByNameFunc(func(f string) bool {
				return strings.ToLower(f) == name
			}).Set(val.Elem())
		case reflect.Map:
			ve.SetMapIndex(reflect.ValueOf(name), val.Elem())
		}
		valMap[valMapPrefix+"."+strings.Join(names[:l-i-1], ".")] = v
		val = v
	}
	field.SetMapIndex(reflect.ValueOf(names[0]), val.Elem())
}

// support multiple name casting: snake, lowercases
func getField(v reflect.Value, schema string) (f reflect.Value, pos int) {
	// TODO: indirect(v) here is overkill?
	names := strings.Split(schema, ".")
	for _, name := range names {
		v = indirect(v)
		// if vk := v.Kind(); vk == reflect.Map {
		// 	continue
		// } else
		if v.Kind() != reflect.Struct {
			break
		}

		num := v.NumField()
		vt := v.Type()
		for i := 0; i < num; i++ {
			sf := vt.FieldByIndex([]int{i})
			if strings.ToLower(sf.Name) == name {
				f = v.FieldByIndex([]int{i})
				break
			}
		}

		if f.IsValid() {
			v = f
			pos++
			continue
		}

		for i := 0; i < num; i++ {
			if sf := vt.FieldByIndex([]int{i}); indirectT(sf.Type).Kind() == reflect.Struct {
				f, _ = getField(v.FieldByIndex([]int{i}), name)
			}
			if f.IsValid() {
				break
			}
		}

		if !f.IsValid() {
			break
		}

		v = f
		pos++
	}

	// if f.Kind() != reflect.Invalid {
	// 	return
	// }

	return
}

// indirect initializes value if necessary
func indirect(v reflect.Value) reflect.Value {
	if v.Kind() != reflect.Ptr {
		return v
	}

	if v.IsNil() {
		v.Set(newValue(v.Type()).Elem())
	}

	for {
		v = v.Elem()
		if v.Kind() != reflect.Ptr {
			break
		}

		if v.IsNil() {
			v.Set(newValue(v.Type()).Elem())
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
	key = newValue(keyt).Elem()
	valet := indirectT(valt)
	if k := valet.Kind(); k == reflect.Array || k == reflect.Slice {
		switch nvalet := indirectT(valet.Elem()); nvalet.Kind() {
		case reflect.Struct, reflect.Slice, reflect.Array, reflect.Map:
			valt = valet.Elem()
			valet = nvalet
		}
	}

	for i, col := range columns {
		if col == mapKey || strings.HasPrefix(col, mapKey+".") {
			if key.Kind() == reflect.Ptr {
				key = indirect(key)
			}
			if key.Kind() == reflect.Struct {
				col = strings.Replace(col, mapKey+".", "", 1)
				f, _ := getField(key, col)
				fields = append(fields, f.Addr().Interface())
			} else {
				fields = append(fields, key.Addr().Interface())
			}
			continue
		}

		switch valet.Kind() {
		case reflect.Struct:
			if val == nil {
				val = newValue(valt)
			}
			f, _ := getField(val.(reflect.Value), col)
			fields = append(fields, f.Addr().Interface())
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
		case reflect.Interface:
			fallthrough
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
