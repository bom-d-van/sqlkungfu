package sqlkungfu

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type Person struct {
	Id        uint64
	FirstName string
	LastName  ***string
}

func (p Person) String() string {
	return p.FirstName + " " + ***p.LastName
}

func TestUnmarshalStruct(t *testing.T) {
	db, err := sql.Open("sqlite3", "test.db")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id integer PRIMARY KEY,
			lastname varchar(255),
			firstname varchar(255)
		);

		INSERT INTO persons (firstname, lastname)
		VALUES ("kungfu", "master");
	`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select * from persons")
	if err != nil {
		t.Fatal(err)
	}

	var p Person
	pp := &p
	ppp := &pp
	err = Unmarshal(rows, &ppp)
	if err != nil {
		t.Error(err)
	}

	if p.String() != "kungfu master" {
		t.Errorf("got %s; want %s", p.String(), "kungfu master")
	}
}

func TestUnmarshalSlice(t *testing.T) {
	db, err := sql.Open("sqlite3", "test.db")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id integer PRIMARY KEY,
			lastname varchar(255),
			firstname varchar(255)
		);

		INSERT INTO persons (firstname, lastname) VALUES ("kungfu0", "master0");
		INSERT INTO persons (firstname, lastname) VALUES ("kungfu1", "master1");
	`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select * from persons")
	if err != nil {
		t.Fatal(err)
	}

	var ps []**Person
	err = Unmarshal(rows, &ps)
	if err != nil {
		t.Error(err)
	}

	if len(ps) != 2 {
		t.Errorf("got %d persons; want 2", len(ps))
	}
	for i, p := range ps {
		if want := fmt.Sprintf("kungfu%d master%d", i, i); (**p).String() != want {
			t.Errorf("got %s; want %s", (**p).String(), want)
		}
	}
}

// func BenchmarkNewValue(b *testing.B) {
// 	v := 1
// 	vv := &v
// 	vvv := &vv
// 	vvvv := &vvv
// 	vvvvv := &vvvv
// 	vvvvvv := &vvvvv
// 	vvvvvvv := &vvvvvv
// 	p := &vvvvvvv
// 	for i := 0; i < b.N; i++ {
// 		newValue(reflect.TypeOf(p))
// 	}
// }

// func BenchmarkNewValueRecursive(b *testing.B) {
// 	v := 1
// 	vv := &v
// 	vvv := &vv
// 	vvvv := &vvv
// 	vvvvv := &vvvv
// 	vvvvvv := &vvvvv
// 	vvvvvvv := &vvvvvv
// 	p := &vvvvvvv
// 	for i := 0; i < b.N; i++ {
// 		newValueRecursive(reflect.TypeOf(p))
// 	}
// }
