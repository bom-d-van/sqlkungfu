package sqlkungfu

import "database/sql"

// http://godoc.org/github.com/jmoiron/sqlx

type Master struct {
	SchemaSeparator  string
	MapKey           string
	MapUint8ToString bool

	// TODO: with tag support
	FieldNameMap func(s interface{}, name string) string
}

var DefaultMaster = Master{
	// FieldNameMap:     func(name string) string { return strings.ToLower(name) },
	SchemaSeparator:  ".",
	MapKey:           "sqlmapkey",
	MapUint8ToString: true,
}

func Unmarshal(rows *sql.Rows, v interface{}) (err error) {
	return DefaultMaster.Unmarshal(rows, v)
}
