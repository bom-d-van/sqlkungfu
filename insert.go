package sqlkungfu

import (
	"fmt"
	"reflect"
	"strings"

	"database/sql"
)

type (
	// InsertNull bool
	TableName string
)

func Insert(db *sql.DB, v interface{}, cfgs ...interface{}) (string, sql.Result, error) {
	return DefaultMaster.Insert(db, v, cfgs...)
}

// INSERT INTO table_name (column1,column2,column3,...)
// VALUES (value1,value2,value3,...);
// TODO:
// - map
func (m Master) Insert(db *sql.DB, v interface{}, cfgs ...interface{}) (insert string, r sql.Result, err error) {
	rv := indirect(reflect.ValueOf(v))

	table := strings.ToLower(rv.Type().Name()) + "s"
	// var insertNull bool
	for _, c := range cfgs {
		switch c.(type) {
		// case InsertNull:
		// 	insertNull = true
		case TableName:
			table = string(c.(TableName))
		}
	}

	// _ = insertNull
	var (
		idField reflect.Value
		fields  []string
		holders []string
		values  []interface{}
	)
	switch rv.Kind() {
	case reflect.Struct:
		idField, fields, holders, values, err = m.walkStruct(rv)
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			err = fmt.Errorf("sqlkungfu: Insert(map key is %s, want string)", rv.Type().Key().Kind())
			return
		}
		for _, k := range rv.MapKeys() {
			fields = append(fields, m.quoteColumn(k.String()))
			holders = append(holders, "?")
			values = append(values, rv.MapIndex(k).Interface())
		}
	}
	if err != nil {
		return
	}
	insert = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(fields, ","), strings.Join(holders, ","))

	if r, err = db.Exec(insert, values...); err != nil {
		return
	}
	if idField.IsValid() && idField.CanSet() || rv.Kind() == reflect.Map {
		var id int64
		if id, err = r.LastInsertId(); err != nil {
			return
		}
		if rv.Kind() == reflect.Map {
			rv.SetMapIndex(reflect.ValueOf("id"), reflect.ValueOf(id))
		} else {
			idField.Set(reflect.ValueOf(id).Convert(idField.Type()))
		}
	}
	return
}

func (m Master) walkStruct(rv reflect.Value) (idField reflect.Value, fields []string, holders []string, values []interface{}, err error) {
	rvt := rv.Type()
	num := rv.NumField()
	for i := 0; i < num; i++ {
		st := rvt.FieldByIndex([]int{i})
		options := strings.Split(st.Tag.Get(m.TagKey), ",")
		for i, o := range options {
			options[i] = strings.TrimSpace(o)
		}
		name := options[0]
		if name == "-" {
			continue
		} else if name == "" {
			name = strings.ToLower(st.Name)
		}

		options = options[1:]
		field := rv.FieldByIndex([]int{i})
		ftype := indirectT(st.Type).Kind()
		if ftype == reflect.Struct && (st.Anonymous || optionsContain(options, "inline")) {
			id, f, h, v, e := m.walkStruct(indirect(field))
			if e != nil {
				err = e
				return
			}
			if id.IsValid() && !idField.IsValid() {
				idField = id
			}

			fields = append(fields, f...)
			holders = append(holders, h...)
			values = append(values, v...)
			continue
		}

		switch ftype {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if optionsContain(options, "id") || strings.ToLower(st.Name) == "id" {
				idField = indirect(field)
				continue
			}
		case reflect.Struct, reflect.Array, reflect.Map, reflect.Slice:
			continue
		}

		values = append(values, field.Interface())
		fields = append(fields, m.quoteColumn(name))
		holders = append(holders, "?")
	}

	return
}

func optionsContain(options []string, tag string) bool {
	for _, o := range options {
		if o == tag {
			return true
		}
	}
	return false
}

// UPDATE table_name
// SET column1=value1,column2=value2,...
// WHERE some_column=some_value;
func (m Master) Update(db *sql.DB, v interface{}) (string, error) {
	return "", nil
}
