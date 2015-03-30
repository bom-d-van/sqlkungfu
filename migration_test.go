package sqlkungfu

import (
	"reflect"
	"testing"
)

func TestMigration(t *testing.T) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS persons;DROP TABLE IF EXISTS sqlkungfu_migrations;`); err != nil {
		t.Fatal(err)
	}

	if err := InitMigration(db); err != nil {
		t.Error(err)
	}

	query := `CREATE TABLE persons(id integer, name varchar(255), PRIMARY KEY (id))`
	m1, _, err := Exec(db, query)
	if err != nil {
		t.Error(err)
	}
	if want := "d64944d68946c2b3c0e6d452ad6831a0"; m1.Checksum != want {
		t.Errorf("m1.Checksum = %s; want %s", m1.Checksum, want)
	}

	m2, _, err := Exec(db, query)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(m1, m2) {
		t.Error("m1(%s) != m2(%s)")
	}

	m3, _, err := Exec(db, "ALTER TABLE persons ADD COLUMN new_column string")
	if err != nil {
		t.Error(err)
	}
	if want := "09ea8f5b89691a3278772f772db907a9"; m3.Checksum != want {
		t.Errorf("m3.Checksum = %s; want %s", m3.Checksum, want)
	}

	var tableInfo []map[string]interface{}
	rows, err := db.Query("PRAGMA table_info(persons)")
	if err != nil {
		t.Error(err)
	}
	err = Unmarshal(rows, &tableInfo)
	if err != nil {
		t.Error(err)
	}
	want := []string{"id", "name", "new_column"}
	for i, info := range tableInfo {
		if info["name"] != want[i] {
			t.Errorf("tableInfo[%d] = %s; want %s", i, info["name"], want[i])
		}
	}
}
