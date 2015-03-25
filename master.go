package sqlkungfu

import "database/sql"

// http://godoc.org/github.com/jmoiron/sqlx

type Master struct {
	SchemaSeparator  string
	MapKey           string
	MapUint8ToString bool
	TagKey           string

	// http://blog.christosoft.de/2012/10/sqlite-escaping-table-acolumn-names/
	// http://stackoverflow.com/questions/2901453/sql-standard-to-escape-column-names
	QuoteColumn func(string) string

	// TODO: with tag support
	ColumnMap    func(s interface{}, name string) string
	FieldNameMap func(s interface{}, name string) string
}

var DefaultMaster = Master{
	// FieldNameMap:     func(name string) string { return strings.ToLower(name) },
	SchemaSeparator:  ".",
	MapKey:           "sqlmapkey",
	TagKey:           "sqlkungfu",
	MapUint8ToString: true,
	QuoteColumn: func(c string) string {
		return "`" + c + "`"
	},
}

func NewMaster() Master {
	return Master{TagKey: "sqlkungfu"}
}

func Unmarshal(rows *sql.Rows, v interface{}) (err error) {
	return DefaultMaster.Unmarshal(rows, v)
}

func (m Master) quoteColumn(c string) string {
	if m.QuoteColumn != nil {
		return m.QuoteColumn(c)
	}
	return c
}
