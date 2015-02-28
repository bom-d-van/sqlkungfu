package sqlkungfu

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type Person struct {
	Id        uint64
	FirstName string
	LastName  ***string
	Dream     *Dream
	Projects  []*Project
	Age       Int
	Address
}

type Int int

type Address struct {
	Addr string
}

type Dream struct {
	Note string
}

type Project struct {
	Name string
}

func (p Person) String() string {
	return p.FirstName + " " + ***p.LastName
}

var db *sql.DB

func init() {
	var err error
	db, err = sql.Open("sqlite3", "test.db")
	if err != nil {
		panic(err)
	}
}

// TODO:
// - fix error: database is locked
func TestUnmarshalStruct(t *testing.T) {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id integer PRIMARY KEY,
			lastname varchar(255),
			firstname varchar(255),
			age integer,
			addr string
		);

		INSERT INTO persons (firstname, lastname, age, addr) VALUES ("kungfu", "master", 24, "Shaolin Temple");
	`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select * from persons")
	if err != nil {
		t.Fatal(err)
	}

	// TODO: var p **Person
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
	if p.Age != Int(24) {
		t.Errorf("got %d; want 24", p.Age)
	}
	if p.Addr != "Shaolin Temple" {
		t.Errorf("got %q; want %q", p.Addr, "Shaolin Temple")
	}
}

func TestUnmarshalEmptyPointer(t *testing.T) {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id integer PRIMARY KEY,
			lastname varchar(255),
			firstname varchar(255),
			age integer,
			addr string
		);

		INSERT INTO persons (firstname, lastname, age, addr) VALUES ("kungfu", "master", 24, "Shaolin Temple");
	`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select * from persons")
	if err != nil {
		t.Fatal(err)
	}

	var pp **Person
	err = Unmarshal(rows, &pp)
	if err != nil {
		t.Error(err)
	}

	p := **pp
	if p.String() != "kungfu master" {
		t.Errorf("got %s; want %s", p.String(), "kungfu master")
	}
	if p.Age != Int(24) {
		t.Errorf("got %d; want 24", p.Age)
	}
	if p.Addr != "Shaolin Temple" {
		t.Errorf("got %q; want %q", p.Addr, "Shaolin Temple")
	}
}

func TestUnmarshalSlicePlain(t *testing.T) {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id integer PRIMARY KEY,
			lastname varchar(255),
			firstname varchar(255)
		);

		INSERT INTO persons (firstname, lastname) VALUES ("kungfu", "master");
	`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select firstname from persons")
	if err != nil {
		t.Fatal(err)
	}

	var ps []string
	err = Unmarshal(rows, &ps)
	if err != nil {
		t.Error(err)
	}

	if len(ps) != 1 {
		t.Errorf("retrieve %d; want 1", len(ps))
		return
	}
	if ps[0] != "kungfu" {
		t.Errorf("got %s; want %s", ps[0], "kungfu")
	}
}

func TestUnmarshalSliceSlice(t *testing.T) {
	_, err := db.Exec(`
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

	{
		rows, err := db.Query("select firstname, lastname from persons")
		if err != nil {
			t.Fatal(err)
		}

		var ps [][]string
		err = Unmarshal(rows, &ps)
		if err != nil {
			t.Error(err)
		}

		if len(ps) != 2 {
			t.Errorf("retrieve %d; want 2", len(ps))
			return
		}
		for i, p := range ps {
			if want := fmt.Sprintf("kungfu%d master%d", i, i); strings.Join(p, " ") != want {
				t.Errorf("got %s; want %s", strings.Join(p, " "), want)
			}
		}
	}

	{
		rows, err := db.Query("select firstname, lastname from persons")
		if err != nil {
			t.Fatal(err)
		}

		var ps []**[]string
		err = Unmarshal(rows, &ps)
		if err != nil {
			t.Error(err)
		}

		if len(ps) != 2 {
			t.Errorf("retrieve %d; want 2", len(ps))
			return
		}
		for i, p := range ps {
			if want := fmt.Sprintf("kungfu%d master%d", i, i); strings.Join(**p, " ") != want {
				t.Errorf("got %s; want %s", strings.Join(**p, " "), want)
			}
		}
	}
}

func TestUnmarshalSliceMap(t *testing.T) {
	_, err := db.Exec(`
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

	rows, err := db.Query("select lastname as sqlmapkey, firstname from persons")
	if err != nil {
		t.Fatal(err)
	}

	var ps []map[string]string
	err = Unmarshal(rows, &ps)
	if err != nil {
		t.Error(err)
	}

	if len(ps) != 2 {
		t.Errorf("len(ps) = %d; want 2", len(ps))
	}
	for i, p := range ps {
		key := fmt.Sprintf("master%d", i)
		if got, ok := p[key]; ok {
			if want := fmt.Sprintf("kungfu%d", i); got != want {
				t.Errorf(`ps["master0"] %q; want %q`, got, want)
			}
		} else {
			t.Errorf("p[%s] do not exist", key)
		}
	}

	// if got, ok := ps["master1"]; ok {
	// 	if got != "kungfu1" {
	// 		t.Errorf(`ps["master1"] %q; want "kungfu1"`, got)
	// 	}
	// } else {
	// 	t.Error(`"ps"[master1] does not exist`)
	// }
}

func TestUnmarshalSliceMapSlice(t *testing.T) {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id 			integer PRIMARY KEY,
			lastname 	varchar(255),
			firstname 	varchar(255),
			sex 		varchar
		);

		INSERT INTO persons (firstname, lastname, sex) VALUES ("kungfu0", "master0", "female");
		INSERT INTO persons (firstname, lastname, sex) VALUES ("kungfu1", "master1", "female");
	`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
	if err != nil {
		t.Fatal(err)
	}

	var ps []map[string][]string
	err = Unmarshal(rows, &ps)
	if err != nil {
		t.Error(err)
	}

	if len(ps) != 2 {
		t.Errorf("len(ps) = %d; want 2", len(ps))
	}
	for i, p := range ps {
		key := fmt.Sprintf("master%d", i)
		if got, ok := p[key]; ok {
			want := fmt.Sprintf("kungfu%d female", i)
			if g := strings.Join(got, " "); g != want {
				t.Errorf(`ps[%q] %q; want %q`, key, g, want)
			}
		} else {
			t.Errorf("ps[%s] do not exist", key)
		}
	}
}

func TestUnmarshalSliceStruct(t *testing.T) {
	_, err := db.Exec(`
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
		return
	}
	for i, p := range ps {
		if want := fmt.Sprintf("kungfu%d master%d", i, i); (**p).String() != want {
			t.Errorf("got %s; want %s", (**p).String(), want)
		}
	}
}

func TestUnmarshalArray(t *testing.T) {
	_, err := db.Exec(`
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

	var ps [2]**Person
	err = Unmarshal(rows, &ps)
	if err != nil {
		t.Error(err)
	}

	if len(ps) != 2 {
		t.Errorf("got %d persons; want 2", len(ps))
		return
	}
	for i, p := range ps {
		if want := fmt.Sprintf("kungfu%d master%d", i, i); (**p).String() != want {
			t.Errorf("got %s; want %s", (**p).String(), want)
		}
	}
}

func TestUnmarshalMap(t *testing.T) {
	_, err := db.Exec(`
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

	rows, err := db.Query("select lastname as sqlmapkey, firstname from persons")
	if err != nil {
		t.Fatal(err)
	}

	ps := map[string]string{}
	err = Unmarshal(rows, &ps)
	if err != nil {
		t.Error(err)
	}

	if got, ok := ps["master0"]; ok {
		if got != "kungfu0" {
			t.Errorf(`ps["master0"] %q; want "kungfu0"`, got)
		}
	} else {
		t.Error(`ps["master0"] does not exist`)
	}
	if got, ok := ps["master1"]; ok {
		if got != "kungfu1" {
			t.Errorf(`ps["master1"] %q; want "kungfu1"`, got)
		}
	} else {
		t.Error(`ps["master1"] does not exist`)
	}
}

func TestUnmarshalMapSlice(t *testing.T) {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id 			integer PRIMARY KEY,
			lastname 	varchar(255),
			firstname 	varchar(255),
			sex 		varchar
		);

		INSERT INTO persons (firstname, lastname, sex) VALUES ("kungfu0", "master0", "male");
		INSERT INTO persons (firstname, lastname, sex) VALUES ("kungfu1", "master1", "female");
	`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
	if err != nil {
		t.Fatal(err)
	}

	ps := map[string][]string{}
	err = Unmarshal(rows, &ps)
	if err != nil {
		t.Error(err)
	}

	if got, ok := ps["master0"]; ok {
		if g := strings.Join(got, " "); g != "kungfu0 male" {
			t.Errorf(`ps["master0"] %q; want "kungfu0 male"`, g)
		}
	} else {
		t.Error(`ps["master0"] does not exist`)
	}
	if got, ok := ps["master1"]; ok {
		if g := strings.Join(got, " "); g != "kungfu1 female" {
			t.Errorf(`ps["master1"] %q; want "kungfu1 female"`, g)
		}
	} else {
		t.Error(`ps["master1"] does not exist`)
	}
}

func TestUnmarshalMapStruct(t *testing.T) {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id 			integer PRIMARY KEY,
			lastname 	varchar(255),
			firstname 	varchar(255),
			sex 		varchar
		);

		INSERT INTO persons (firstname, lastname, sex) VALUES ("kungfu0", "master0", "male");
		INSERT INTO persons (firstname, lastname, sex) VALUES ("kungfu1", "master1", "female");
	`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
	if err != nil {
		t.Fatal(err)
	}

	ps := map[string]struct {
		FirstName string
		Sex       string
	}{}
	err = Unmarshal(rows, &ps)
	if err != nil {
		t.Error(err)
	}

	if got, ok := ps["master0"]; ok {
		if g := got.FirstName + " " + got.Sex; g != "kungfu0 male" {
			t.Errorf(`ps["master0"] %q; want "kungfu0 male"`, g)
		}
	} else {
		t.Error(`ps["master0"] does not exist`)
	}
	if got, ok := ps["master1"]; ok {
		if g := got.FirstName + " " + got.Sex; g != "kungfu1 female" {
			t.Errorf(`ps["master1"] %q; want "kungfu1 female"`, g)
		}
	} else {
		t.Error(`ps["master1"] does not exist`)
	}
}

func TestUnmarshalMapMap(t *testing.T) {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id 			integer PRIMARY KEY,
			lastname 	varchar(255),
			firstname 	varchar(255),
			sex 		varchar,
			age 		integer
		);

		INSERT INTO persons (firstname, lastname, sex, age) VALUES ("kungfu0", "master0", "male", 24);
		INSERT INTO persons (firstname, lastname, sex, age) VALUES ("kungfu1", "master1", "female", 24);
	`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
	if err != nil {
		t.Fatal(err)
	}

	var ps map[string]map[string]string
	err = Unmarshal(rows, &ps)
	if err != nil {
		t.Error(err)
	}

	if got, ok := ps["master0"]; ok {
		if g := got["firstname"] + " " + got["sex"]; g != "kungfu0 male" {
			t.Errorf(`ps["master0"] %q; want "kungfu0 male"`, g)
		}
	} else {
		t.Error(`ps["master0"] does not exist`)
	}
	if got, ok := ps["master1"]; ok {
		if g := got["firstname"] + " " + got["sex"]; g != "kungfu1 female" {
			t.Errorf(`ps["master1"] %q; want "kungfu1 female"`, g)
		}
	} else {
		t.Error(`ps["master1"] does not exist`)
	}
}

func TestUnmarshalMapSliceStruct(t *testing.T) {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id 			integer PRIMARY KEY,
			lastname 	varchar(255),
			firstname 	varchar(255),
			sex 		varchar
		);

		INSERT INTO persons (firstname, lastname, sex) VALUES ("kungfu0-0", "master0", "male");
		INSERT INTO persons (firstname, lastname, sex) VALUES ("kungfu0-1", "master0", "male");
		INSERT INTO persons (firstname, lastname, sex) VALUES ("kungfu1", "master1", "female");
	`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
	if err != nil {
		t.Fatal(err)
	}

	ps := map[string][]struct {
		FirstName string
		Sex       string
	}{}
	err = Unmarshal(rows, &ps)
	if err != nil {
		t.Error(err)
	}

	if got, ok := ps["master0"]; ok {
		if len(got) != 2 {
			t.Errorf(`len(ps["master0"]) = %d; want 2`, len(got))
		}
		for i, p := range got {
			want := fmt.Sprintf("kungfu0-%d male", i)
			if gotstr := p.FirstName + " " + p.Sex; gotstr != want {
				t.Errorf(`ps["master0"][%d] = %q; want %q`, i, gotstr, want)
			}
		}
	} else {
		t.Error(`ps["master0"] does not exist`)
	}
	if got, ok := ps["master1"]; ok {
		if len(got) != 1 {
			t.Errorf(`len(ps["master1"]) = %d; want 1`, len(got))
		}
		for i, p := range got {
			want := "kungfu1 female"
			if gotstr := p.FirstName + " " + p.Sex; gotstr != want {
				t.Errorf(`ps["master1"][%d] = %q; want %q`, i, gotstr, want)
			}
		}
	} else {
		t.Error(`ps["master1"] does not exist`)
	}
}

func TestUnmarshalMapArray(t *testing.T) {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id 			integer PRIMARY KEY,
			lastname 	varchar(255),
			firstname 	varchar(255),
			sex 		varchar
		);

		INSERT INTO persons (firstname, lastname, sex) VALUES ("kungfu0", "master0", "male");
		INSERT INTO persons (firstname, lastname, sex) VALUES ("kungfu1", "master1", "female");
	`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
	if err != nil {
		t.Fatal(err)
	}

	ps := map[string][2]string{}
	err = Unmarshal(rows, &ps)
	if err != nil {
		t.Error(err)
	}

	if got, ok := ps["master0"]; ok {
		if g := strings.Join(got[:], " "); g != "kungfu0 male" {
			t.Errorf(`ps["master0"] %q; want "kungfu0 male"`, g)
		}
	} else {
		t.Error(`ps["master0"] does not exist`)
	}
	if got, ok := ps["master1"]; ok {
		if g := strings.Join(got[:], " "); g != "kungfu1 female" {
			t.Errorf(`ps["master1"] %q; want "kungfu1 female"`, g)
		}
	} else {
		t.Error(`ps["master1"] does not exist`)
	}
}

func TestNewValue(t *testing.T) {
	var typ map[string]string
	val := newValue(reflect.TypeOf(&typ))
	if val.Elem().Elem().IsNil() {
		t.Error("newValue(*map[string]string) should init a map")
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
