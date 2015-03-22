package sqlkungfu

import "database/sql"

var DefaultMaster = Master{
	// FieldNameMap:     func(name string) string { return strings.ToLower(name) },
	SchemaSeparator:  ".",
	MapKey:           "sqlmapkey",
	MapUint8ToString: true,
}

func Unmarshal(rows *sql.Rows, v interface{}) (err error) {
	return DefaultMaster.Unmarshal(rows, v)
}
