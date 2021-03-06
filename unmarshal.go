package sqlkungfu

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// TODO: handle sql.NullString, i.e. sql.Scanner and driver.Value

var ErrNoRows = errors.New("sqlkungfu: no rows in result set")

// map
// normal
// join
// To-Do:
// - do not override non-nil values
// - convert []uint8 into a string for interface{} values
// - support inline field tag
func (m Master) Unmarshal(rows *sql.Rows, v interface{}) (err error) {
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

	if !rows.Next() {
		err = ErrNoRows
		return
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
		err = m.unmarshalStruct(rows, columns, vv)
	case reflect.Slice, reflect.Array:
		// TODO:
		// - []string
		// - [][]string
		// - []struct{}
		// - []map[string]struct{} ?
		// - []map[string]string{}
		// - []map[string][]string{} ?
		err = m.unmarshalSliceOrArray(vv, rows, columns)
	case reflect.Map:
		err = m.unmarshalMap(vv, rows, columns)
	default:
		return fmt.Errorf("sqlkungfu: Unmarshal(unsupported type %T)", v)
	}

	return
}

func (m Master) unmarshalStruct(rows *sql.Rows, columns []string, v reflect.Value) (err error) {
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
		field, pos := m.getField(v, col)
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
			names := strings.Split(col, m.SchemaSeparator)
			info.names = names[pos:]
			info.Value = field
			index := names[pos-1]
			info.valMapPrefix = index
			val = info.Value
			if val, info.names, err = m.parseColSchema(valMap, val, index, info.names); err != nil {
				return
			}
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

			m.setColSchema(field.names, field.Value, reflect.ValueOf(fields[i]), valMap, field.valMapPrefix)
		}
	}

	return
}

func (m Master) parseColSchema(valMap map[string]reflect.Value, val reflect.Value, index string, names []string) (reflect.Value, []string, error) {
	var tlen int
	for i, name := range names {
		if index != "" {
			index += m.SchemaSeparator
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
						index += m.SchemaSeparator + strings.Join(names[i+1:], m.SchemaSeparator)
					}
				}
				val = newValue(et)
				valMap[index] = val
			}
			tlen++
		}
	}
	names = append(names[:tlen-1], strings.Join(names[tlen-1:], m.SchemaSeparator))
	return val, names, nil
}

func (m Master) setColSchema(names []string, field, val reflect.Value, valMap map[string]reflect.Value, valMapPrefix string) {
	l := len(names)
	// TODO: be configurable (maybe by struct tags?)
	if b, ok := val.Elem().Interface().([]uint8); ok && m.MapUint8ToString {
		s := string(b)
		val = reflect.ValueOf(&s)
	}
	for i, _ := range names[1:] {
		name := names[l-i-1]
		v, _ := valMap[valMapPrefix+m.SchemaSeparator+strings.Join(names[:l-i-1], m.SchemaSeparator)]
		ve := indirect(v)
		switch ve.Kind() {
		case reflect.Struct:
			ve.FieldByNameFunc(func(f string) bool {
				return strings.ToLower(f) == name
			}).Set(val.Elem())
		case reflect.Map:
			ve.SetMapIndex(reflect.ValueOf(name), val.Elem())
		}
		valMap[valMapPrefix+m.SchemaSeparator+strings.Join(names[:l-i-1], m.SchemaSeparator)] = v
		val = v
	}
	field.SetMapIndex(reflect.ValueOf(names[0]), val.Elem())
}

// support multiple name casting: snake, lowercases
func (m Master) getField(v reflect.Value, schema string) (f reflect.Value, pos int) {
	// TODO: indirect(v) here is overkill?
	names := strings.Split(schema, m.SchemaSeparator)
	for _, name := range names {
		v = indirect(v)
		if v.Kind() != reflect.Struct {
			break
		}

		num := v.NumField()
		vt := v.Type()
		for i := 0; i < num; i++ {
			sf := vt.FieldByIndex([]int{i})
			options := strings.Split(sf.Tag.Get("sqlkungfu"), ",")
			if options[0] == name || strings.ToLower(sf.Name) == name {
				f = v.FieldByIndex([]int{i})
				break
			}

			if indirectT(sf.Type).Kind() == reflect.Struct && (sf.Anonymous || optionsContain(options[1:], "inline") || len(names) > 1) {
				if f, _ = m.getField(v.FieldByIndex([]int{i}), name); f.IsValid() {
					break
				}
			}
		}

		if f.IsValid() {
			v = f
			pos++
			continue
		}
	}

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

func (m Master) unmarshalSliceOrArray(vv reflect.Value, rows *sql.Rows, columns []string) (err error) {
	vet := vv.Type().Elem()
	var arri int
	for {
		e := newValue(vet)
		ee := indirect(e)
		switch ee.Kind() {
		case reflect.Struct:
			if err = m.unmarshalStruct(rows, columns, ee); err != nil {
				return
			}
		case reflect.Map:
			// TODO: is it correct to disable group (MapKey) support here
			keyt := vet.Key()
			valt := vet.Elem()
			fields, _, val := m.genMapFields(columns, keyt, valt)
			if err = rows.Scan(fields...); err != nil {
				return
			}

			ee.Set(reflect.MakeMap(ee.Type()))
			switch indirectT(valt).Kind() {
			case reflect.Map:
				// TODO: schema?
			case reflect.Interface:
				// vals := val.([]reflect.Value)
				// for i, col := range columns {
				// 	v := vals[i].Elem()
				// 	key := reflect.ValueOf(col)
				// 	if b, ok := v.Interface().([]uint8); ok && m.MapUint8ToString {
				// 		ee.SetMapIndex(key, reflect.ValueOf(string(b)))
				// 	} else {
				// 		ee.SetMapIndex(key, v)
				// 	}
				// }
				m.assignMapInterfaceFields(ee, columns, fields)
			default:
				vals := val.([]reflect.Value)
				for i, col := range columns {
					key := reflect.ValueOf(col)
					ee.SetMapIndex(key, vals[i].Elem())
				}
			}
		case reflect.Slice, reflect.Array:
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
			if ee.Kind() == reflect.Slice {
				ee.Set(reflect.Append(ee, eis...))
			} else {
				for i, ei := range eis {
					if b, ok := ei.Interface().([]uint8); ok && m.MapUint8ToString {
						ee.Index(i).Set(reflect.ValueOf(string(b)))
					} else {
						ee.Index(i).Set(ei)
					}
				}
			}
		default:
			if err = rows.Scan(e.Interface()); err != nil {
				return
			}
		}
		if vv.Kind() == reflect.Array {
			vv.Index(arri).Set(e.Elem())
			arri++
		} else {
			vv.Set(reflect.Append(vv, e.Elem()))
		}

		// TODO: explain
		if !rows.Next() {
			break
		}
	}

	return
}

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
func (m Master) unmarshalMap(vv reflect.Value, rows *sql.Rows, columns []string) (err error) {
	vvt := vv.Type()
	keyt := vvt.Key()
	valt := vvt.Elem()
	arri := map[interface{}]int{}
	for {
		fields, key, val := m.genMapFields(columns, keyt, valt)

		if err = rows.Scan(fields...); err != nil {
			return
		}

		switch valet := indirectT(valt); valet.Kind() {
		case reflect.Slice:
			v := vv.MapIndex(key)
			v = m.assignMapSliceOrArray(v, valt, valet, columns, 0, val)
			vv.SetMapIndex(key, v)
		case reflect.Array:
			v := vv.MapIndex(key)
			i := arri[key.Interface()]
			v = m.assignMapSliceOrArray(v, valt, valet, columns, i, val)
			arri[key.Interface()] = i + 1
			vv.SetMapIndex(key, v)
		case reflect.Map:
			vals := val.([]reflect.Value)
			v := newValue(valt).Elem()
			ve := v
			if v.Kind() == reflect.Ptr {
				ve = indirect(v)
			}
			for i, col := range columns {
				if col == m.MapKey {
					continue
				}
				ve.SetMapIndex(reflect.ValueOf(col), vals[i])
			}
			vv.SetMapIndex(key, v)
		case reflect.Interface:
			if valet.NumMethod() != 0 {
				vv.SetMapIndex(key, val.(reflect.Value).Elem())
				continue
			}
			m.assignMapInterfaceFields(vv, columns, fields)
		case reflect.Struct:
			vv.SetMapIndex(key, val.(reflect.Value).Elem())
		default:
			// TODO: is this reasonable?
			vv.SetMapIndex(key, val.([]reflect.Value)[0].Elem())
		}

		// TODO: explain
		if !rows.Next() {
			break
		}
	}
	return
}

// TODO: refactor better naming?
func (m Master) genMapFields(columns []string, keyt, valt reflect.Type) (fields []interface{}, key reflect.Value, val interface{}) {
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
		if col == m.MapKey || strings.HasPrefix(col, m.MapKey+m.SchemaSeparator) {
			if key.Kind() == reflect.Ptr {
				key = indirect(key)
			}
			if key.Kind() == reflect.Struct {
				col = strings.Replace(col, m.MapKey+m.SchemaSeparator, "", 1)
				f, _ := m.getField(key, col)
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
			f, _ := m.getField(val.(reflect.Value), col)
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
			if val == nil {
				val = []reflect.Value{}
			}
			v := newValue(valt)
			ve := v
			if v.Kind() == reflect.Ptr {
				ve = indirect(v)
			}
			fields = append(fields, ve.Addr().Interface())
			val = append(val.([]reflect.Value), v)
		}
	}

	return
}

func (m Master) assignMapInterfaceFields(vv reflect.Value, columns []string, fields []interface{}) {
	for i, col := range columns {
		if col == m.MapKey {
			continue
		}
		names := strings.Split(col, m.SchemaSeparator)
		v := vv
		for _, name := range names[:len(names)-1] {
			if v.Kind() == reflect.Interface {
				v = v.Elem()
			}
			nv := v.MapIndex(reflect.ValueOf(name))
			if !nv.IsValid() {
				nv = reflect.MakeMap(reflect.TypeOf(map[string]interface{}{}))
				v.SetMapIndex(reflect.ValueOf(name), nv)
			}
			v = nv
		}
		val := *fields[i].(*interface{})
		if b, ok := val.([]uint8); ok && m.MapUint8ToString {
			val = string(b)
		}
		// v.Interface().(map[string]interface{})[names[len(names)-1]] = val
		if v.Kind() == reflect.Interface {
			v = v.Elem()
		}
		v.SetMapIndex(reflect.ValueOf(names[len(names)-1]), reflect.ValueOf(val))
	}
}

// TODO: complete test suites for array
func (m Master) assignMapSliceOrArray(v reflect.Value, valt, valet reflect.Type, columns []string, arri int, val interface{}) reflect.Value {
	if !v.IsValid() {
		v = newValue(valt).Elem()
	}
	assign := func(val reflect.Value) {
		if v.Kind() == reflect.Ptr {
			ve := indirect(v)
			if ve.Kind() == reflect.Slice {
				ve.Set(reflect.Append(ve, val))
			} else {
				ve.Index(arri).Set(val)
			}
		} else {
			if v.Kind() == reflect.Slice {
				v = reflect.Append(v, val)
			} else {
				// TODO: improvement?
				nv := newValue(valt).Elem()
				reflect.Copy(nv, v)
				nv.Index(arri).Set(val)
				v = nv
			}
		}
	}

	switch indirectT(valet.Elem()).Kind() {
	case reflect.Struct:
		assign(val.(reflect.Value).Elem())
	case reflect.Slice:
		vale := newValue(valet.Elem()).Elem()
		if vale.Kind() == reflect.Ptr {
			valee := indirect(vale)
			valee.Set(reflect.Append(valee, val.([]reflect.Value)...))
		} else {
			vale = reflect.Append(vale, val.([]reflect.Value)...)
		}
		assign(vale)
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
		assign(vale)
	case reflect.Map:
		vals := val.([]reflect.Value)
		vale := newValue(valet.Elem()).Elem()
		valee := vale
		if vale.Kind() == reflect.Ptr {
			valee = indirect(vale)
		}
		for i, col := range columns {
			if col == m.MapKey {
				continue
			}
			valee.SetMapIndex(reflect.ValueOf(col), vals[i])
		}
		assign(vale)
	default:
		if v.Kind() == reflect.Ptr {
			ve := indirect(v)
			if ve.Kind() == reflect.Slice {
				ve.Set(reflect.Append(ve, val.([]reflect.Value)...))
			} else {
				for i, e := range val.([]reflect.Value) {
					ve.Index(i).Set(e)
				}
			}
		} else {
			if v.Kind() == reflect.Slice {
				v = reflect.Append(v, val.([]reflect.Value)...)
			} else {
				for i, e := range val.([]reflect.Value) {
					v.Index(i).Set(e)
				}
			}
		}
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

// func Columns(v interface{}) (cols []string) {
// 	vt := reflect.TypeOf(v)
// 	count := vt.NumField()
// 	for i := 0; i < count; i++ {
// 		field := vt.Field(i)
// 		cols = append(cols, strings.ToLower(field.Name))
// 	}

// 	return nil
// }

// func ColumnsPrefix(v interface{}, prefix string) []string {
// 	return nil
// }
