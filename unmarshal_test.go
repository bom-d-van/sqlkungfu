package sqlkungfu

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TODO:
// - fix error: database is locked
// - refactor tests
//     - consistent style (got and want, etc)
//     - use reportErrIfNotEqual

type Person struct {
	Id        uint64
	FirstName string
	LastName  ***string
	Dream     *Dream
	Projects  []*Project
	Age       Int
	Address
	CreatedAt time.Time
	FakeTime  time.Time

	NullString sql.NullString
}

type FakeTime time.Time

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

// type unmarshalCase struct {
// 	query string
// 	got   interface{}
// 	want  interface{}
// }

// var unmarshalCases = []unmarshalCase{
// 	func() unmarshalCase {
// 		var person **Person

// 		last := "master"
// 		lastp := &last
// 		lastpp := &lastp
// 		return unmarshalCase{
// 			"select * from persons",
// 			person,
// 			&Person{
// 				Id:        1,
// 				FirstName: "kungfu",
// 				LastName:  &lastpp,
// 				Age:       24,
// 				Address:   Address{Addr: "Shaolin Temple"},
// 			},
// 		}
// 	}(),
// }

// func TestUnmarshal(t *testing.T) {
// 	_, err := db.Exec(`
// 		DROP TABLE IF EXISTS persons;

// 		CREATE TABLE persons(
// 			id integer PRIMARY KEY,
// 			lastname varchar(255),
// 			firstname varchar(255),
// 			age integer,
// 			addr string
// 		);

// 		INSERT INTO persons (firstname, lastname, age, addr) VALUES ("kungfu", "master", 24, "Shaolin Temple");
// 	`)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	for _, c := range unmarshalCases {
// 		rows, err := db.Query(c.query)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		err = Unmarshal(rows, &c.got)
// 		if err != nil {
// 			t.Error(err)
// 		}
// 		if reflect.DeepEqual(c.got, c.want) {
// 			reportErr(t, c.got, c.want)
// 		}
// 	}
// }

func TestUnmarshalStruct(t *testing.T) {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id integer PRIMARY KEY,
			lastname varchar(255),
			firstname varchar(255),
			age integer,
			addr string,
			createdat timestamp,
			faketime timestamp,
			nullstring string
		);

		INSERT INTO persons (firstname, lastname, age, addr, createdat, faketime, nullstring) VALUES ("kungfu", "master", 24, "Shaolin Temple", "2015-03-29 05:29:39.750515459", "2015-03-29 05:29:39.750515459", "null");
	`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select * from persons")
	if err != nil {
		t.Fatal(err)
	}

	var got **Person
	err = Unmarshal(rows, &got)
	if err != nil {
		t.Error(err)
	}

	createdAt, err := time.Parse("2006-01-02T15:04:05.999999999-07:00", "2015-03-29T13:29:39.750515459+08:00")
	if err != nil {
		t.Error(err)
	}
	fakeTime, err := time.Parse("2006-01-02T15:04:05.999999999-07:00", "2015-03-29T13:29:39.750515459+08:00")
	if err != nil {
		t.Error(err)
	}

	last := "master"
	lastp := &last
	lastpp := &lastp
	want := Person{
		Id:        1,
		FirstName: "kungfu",
		LastName:  &lastpp,
		Age:       24,
		Address:   Address{Addr: "Shaolin Temple"},
		CreatedAt: createdAt,
		FakeTime:  fakeTime,
		NullString: sql.NullString{
			String: "null",
			Valid:  true,
		},
	}
	if r := reportErrIfNotEqual(t, **got, want); r != "" {
		t.Error(r)
	}
}

// TODO:
// - map[string][]interface{}
func TestUnmarshalStructWithMutiSameColumn(t *testing.T) {
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

	{
		rows, err := db.Query("select firstname name, lastname name from persons")
		if err != nil {
			t.Fatal(err)
		}

		var p struct {
			Name *[]*string
		}
		err = Unmarshal(rows, &p)
		if err != nil {
			t.Error(err)
		}

		if got := *(*p.Name)[0] + " " + *(*p.Name)[1]; got != "kungfu master" {
			t.Errorf("got %s; want %s", got, "kungfu master")
		}
	}
	{
		rows, err := db.Query("select firstname name, lastname name from persons")
		if err != nil {
			t.Fatal(err)
		}

		var p struct {
			Name [2]string
		}
		err = Unmarshal(rows, &p)
		if err != nil {
			t.Error(err)
		}

		if got := p.Name[0] + " " + p.Name[1]; got != "kungfu master" {
			t.Errorf("got %s; want %s", got, "kungfu master")
		}
	}
	{
		rows, err := db.Query("select firstname 'info.name', lastname 'info.name' from persons")
		if err != nil {
			t.Fatal(err)
		}

		var p struct {
			Info struct {
				Name *[]*string
			}
		}
		err = Unmarshal(rows, &p)
		if err != nil {
			t.Error(err)
		}

		if got := *(*p.Info.Name)[0] + " " + *(*p.Info.Name)[1]; got != "kungfu master" {
			t.Errorf("got %s; want %s", got, "kungfu master")
		}
	}
}

func TestUnmarshalEmbeddedStruct(t *testing.T) {
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

	rows, err := db.Query("select lastname, firstname, age, addr from persons")
	if err != nil {
		t.Fatal(err)
	}

	type Name struct{ LastName, FirstName string }
	type Num struct{ Age int }
	type Text struct{ Addr string }
	type Info struct {
		Num
		*Text
	}
	var p struct {
		Name
		*Info
	}
	err = Unmarshal(rows, &p)
	if err != nil {
		t.Error(err)
	}

	if got := p.FirstName + " " + p.LastName; got != "kungfu master" {
		t.Errorf("got %s; want %s", got, "kungfu master")
	}
	if p.Age != 24 {
		t.Errorf("got %d; want 24", p.Age)
	}
	if p.Addr != "Shaolin Temple" {
		t.Errorf("got %q; want %q", p.Addr, "Shaolin Temple")
	}
}

func TestUnmarshalNestedStruct(t *testing.T) {
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

	rows, err := db.Query("select lastname as 'name.last', firstname as 'name.first', age as 'info.num.age', addr as 'info.text.addr' from persons")
	if err != nil {
		t.Fatal(err)
	}

	type Name struct{ Last, First string }
	var p struct {
		Name **Name
		Info *struct {
			Num struct {
				Age int
			}
			Text struct {
				Addr string
			}
		}
	}
	err = Unmarshal(rows, &p)
	if err != nil {
		t.Error(err)
	}

	if got := (*p.Name).First + " " + (*p.Name).Last; got != "kungfu master" {
		t.Errorf("got %s; want %s", got, "kungfu master")
	}
	if p.Info.Num.Age != 24 {
		t.Errorf("got %d; want 24", p.Info.Num.Age)
	}
	if p.Info.Text.Addr != "Shaolin Temple" {
		t.Errorf("got %q; want %q", p.Info.Text.Addr, "Shaolin Temple")
	}
}

// TODO:
// - Name **map[string]string
// - Info map[struct]interface{}
// - Name map[String]string
// - handle schema with extra dot separator
// - Info map[string][]map[string]interface{}
func TestUnmarshalSchema(t *testing.T) {
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
		INSERT INTO persons (firstname, lastname, age, addr) VALUES ("kungfu", "master", 24, "Shaolin Temple");
	`)
	if err != nil {
		t.Fatal(err)
	}

	{
		rows, err := db.Query("select lastname 'name.last', firstname 'name.first', age 'info.num.age', addr 'info.text.addr' from persons")
		if err != nil {
			t.Fatal(err)
		}

		type data struct {
			Name map[string]string
			Info map[string]map[string]interface{}
		}

		var got data
		err = Unmarshal(rows, &got)
		if err != nil {
			t.Error(err)
		}

		want := data{
			Name: map[string]string{"first": "kungfu", "last": "master"},
			Info: map[string]map[string]interface{}{
				"num":  map[string]interface{}{"age": int64(24)},
				"text": map[string]interface{}{"addr": "Shaolin Temple"},
			},
		}

		if r := reportErrIfNotEqual(t, got, want); r != "" {
			t.Error(r)
		}
	}
	{
		rows, err := db.Query("select lastname 'name.l0.last', firstname 'name.l0.first', age 'info.l0.l1.num.age' from persons")
		if err != nil {
			t.Fatal(err)
		}

		type data struct {
			Name map[string]string
			Info map[string]map[string]int
		}

		var got data
		err = Unmarshal(rows, &got)
		if err != nil {
			t.Error(err)
		}

		want := data{
			Name: map[string]string{"l0.first": "kungfu", "l0.last": "master"},
			Info: map[string]map[string]int{
				"l0": map[string]int{
					"l1.num.age": 24,
				},
			},
		}
		if r := reportErrIfNotEqual(t, got, want); r != "" {
			t.Error(r)
		}
	}
	{
		rows, err := db.Query("select lastname 'name.last', firstname 'name.first', age 'info.num.age', addr 'info.text.addr' from persons")
		if err != nil {
			t.Fatal(err)
		}

		type data struct {
			Name map[string]string
			Info map[string]interface{}
		}
		var got data
		err = Unmarshal(rows, &got)
		if err != nil {
			t.Error(err)
		}

		want := data{
			Name: map[string]string{"first": "kungfu", "last": "master"},
			Info: map[string]interface{}{
				"num":  map[string]interface{}{"age": int64(24)},
				"text": map[string]interface{}{"addr": "Shaolin Temple"},
			},
		}

		if r := reportErrIfNotEqual(t, got, want); r != "" {
			t.Error(r)
		}
	}
	{
		rows, err := db.Query("select lastname 'name.last', firstname 'name.first', age 'info.num.age', addr 'info.text.addr' from persons")
		if err != nil {
			t.Fatal(err)
		}

		type info struct {
			Age  int    // Num
			Addr string // Text
		}
		type data struct {
			Name map[string]string
			Info map[string]info
		}
		var got data

		err = Unmarshal(rows, &got)
		if err != nil {
			t.Error(err)
		}

		want := data{
			Name: map[string]string{"first": "kungfu", "last": "master"},
			Info: map[string]info{
				"num":  info{Age: 24},
				"text": info{Addr: "Shaolin Temple"},
			},
		}

		if r := reportErrIfNotEqual(t, got, want); r != "" {
			t.Error(r)
		}
	}
	{
		rows, err := db.Query("select lastname 'name.last', firstname 'name.first', age 'info.l0.l1.l2.num.age', addr 'info.l0.l1.l2.text.addr' from persons")
		if err != nil {
			t.Fatal(err)
		}

		type Num struct{ Age int }
		type Text struct{ Addr string }
		type L2 struct {
			Num  Num
			Text Text
		}
		type L1 struct{ L2 L2 }
		type val struct{ L1 L1 }
		type info map[string]*val
		type data struct {
			Name map[string]string
			Info info
		}
		var got data

		err = Unmarshal(rows, &got)
		if err != nil {
			t.Error(err)
		}

		want := data{
			Name: map[string]string{"first": "kungfu", "last": "master"},
			Info: info{"l0": &val{L1{L2{
				Num:  Num{24},
				Text: Text{"Shaolin Temple"},
			}}}},
		}

		if r := reportErrIfNotEqual(t, got, want); r != "" {
			t.Error(r)
		}
	}
	{
		rows, err := db.Query("select lastname 'name.last', firstname 'name.first', age 'info.l0.l1.l2.num.age', addr 'info.l0.l1.l2.text.addr' from persons")
		if err != nil {
			t.Fatal(err)
		}

		type Num struct{ Age int }
		type Text struct{ Addr string }
		type L2 struct {
			Num  Num
			Text Text
		}
		type L1 struct{ L2 L2 }
		type val struct{ L1 L1 }
		type info map[string]*val
		type data struct {
			Name map[string]string
			Info info
		}
		var got []data

		err = Unmarshal(rows, &got)
		if err != nil {
			t.Error(err)
		}

		want := []data{
			{
				Name: map[string]string{"first": "kungfu", "last": "master"},
				Info: info{"l0": &val{L1{L2{
					Num:  Num{24},
					Text: Text{"Shaolin Temple"},
				}}}},
			},
			{
				Name: map[string]string{"first": "kungfu", "last": "master"},
				Info: info{"l0": &val{L1{L2{
					Num:  Num{24},
					Text: Text{"Shaolin Temple"},
				}}}},
			},
		}

		if r := reportErrIfNotEqual(t, got, want); r != "" {
			t.Error(r)
		}
	}
	{
		rows, err := db.Query("select lastname 'name.last', firstname 'name.first', age 'info.num.age', addr 'info.text.addr' from persons")
		if err != nil {
			t.Fatal(err)
		}

		type info struct {
			Num  map[string]int
			Text map[string]string
		}
		type data struct {
			Name map[string]string
			Info info
		}
		var got data

		err = Unmarshal(rows, &got)
		if err != nil {
			t.Error(err)
		}

		want := data{
			Name: map[string]string{"first": "kungfu", "last": "master"},
			Info: info{
				Num:  map[string]int{"age": 24},
				Text: map[string]string{"addr": "Shaolin Temple"},
			},
		}

		if r := reportErrIfNotEqual(t, got, want); r != "" {
			t.Error(r)
		}
	}
	{
		rows, err := db.Query("select lastname 'name.last', firstname 'name.first', age 'info.num.age', addr 'info.text.addr' from persons")
		if err != nil {
			t.Fatal(err)
		}

		var got map[string]interface{}

		err = Unmarshal(rows, &got)
		if err != nil {
			t.Error(err)
		}

		want := map[string]interface{}{
			"name": map[string]interface{}{"first": "kungfu", "last": "master"},
			"info": map[string]interface{}{
				"num":  map[string]interface{}{"age": int64(24)},
				"text": map[string]interface{}{"addr": "Shaolin Temple"},
			},
		}

		if r := reportErrIfNotEqual(t, got, want); r != "" {
			t.Error(r)
		}
	}
}

func reportErrIfNotEqual(t *testing.T, got, want interface{}) string {
	if reflect.DeepEqual(got, want) {
		return ""
	}
	var err error
	if got, err = json.MarshalIndent(got, "", "  "); err != nil {
		t.Fatal(err)
	}
	if want, err = json.MarshalIndent(want, "", "  "); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("got %s\nwant %s", got, want)
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

// TODO: []interface{}
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

	{
		rows, err := db.Query("select firstname from persons")
		if err != nil {
			t.Fatal(err)
		}

		// TODO: var ps ****[]string
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
	{
		rows, err := db.Query("select firstname from persons")
		if err != nil {
			t.Fatal(err)
		}

		var ps []interface{}
		err = Unmarshal(rows, &ps)
		if err != nil {
			t.Error(err)
		}

		if len(ps) != 1 {
			t.Errorf("retrieve %d; want 1", len(ps))
			return
		}
		if string(ps[0].([]uint8)) != "kungfu" {
			t.Errorf("got %s; want %s", ps[0], "kungfu")
		}
	}
}

// TODO: [][]interface{}
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
	{
		rows, err := db.Query("select id, firstname, lastname from persons")
		if err != nil {
			t.Fatal(err)
		}

		var ps [][]interface{}
		err = Unmarshal(rows, &ps)
		if err != nil {
			t.Error(err)
		}

		if len(ps) != 2 {
			t.Errorf("retrieve %d; want 2", len(ps))
			return
		}
		for i, p := range ps {
			if got, want := fmt.Sprintf("%d %s %s", p...), fmt.Sprintf("%d kungfu%d master%d", i+1, i, i); got != want {
				t.Errorf("got %s; want %s", got, want)
			}
		}
	}
}

func TestUnmarshalSliceArray(t *testing.T) {
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

		var got [][2]string
		err = Unmarshal(rows, &got)
		if err != nil {
			t.Error(err)
		}

		want := [][2]string{{"kungfu0", "master0"}, {"kungfu1", "master1"}}
		if r := reportErrIfNotEqual(t, got, want); r != "" {
			t.Error(r)
		}
	}
	{
		rows, err := db.Query("select firstname, lastname from persons")
		if err != nil {
			t.Fatal(err)
		}

		var got []**[2]string
		err = Unmarshal(rows, &got)
		if err != nil {
			t.Error(err)
		}

		s0 := &[2]string{"kungfu0", "master0"}
		s1 := &[2]string{"kungfu1", "master1"}
		want := []**[2]string{&s0, &s1}
		if r := reportErrIfNotEqual(t, got, want); r != "" {
			t.Error(r)
		}
	}
	{
		rows, err := db.Query("select id, firstname, lastname from persons")
		if err != nil {
			t.Fatal(err)
		}

		var got [][3]interface{}
		err = Unmarshal(rows, &got)
		if err != nil {
			t.Error(err)
		}

		want := [][3]interface{}{{int64(1), "kungfu0", "master0"}, {int64(2), "kungfu1", "master1"}}
		if r := reportErrIfNotEqual(t, got, want); r != "" {
			t.Error(r)
		}
	}
}

func TestUnmarshalArraySlice(t *testing.T) {
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

		var ps [2][]string
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

		var ps [2]**[]string
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
	{
		rows, err := db.Query("select id, firstname, lastname from persons")
		if err != nil {
			t.Fatal(err)
		}

		var ps [2][]interface{}
		err = Unmarshal(rows, &ps)
		if err != nil {
			t.Error(err)
		}

		if len(ps) != 2 {
			t.Errorf("retrieve %d; want 2", len(ps))
			return
		}
		for i, p := range ps {
			if got, want := fmt.Sprintf("%d %s %s", p...), fmt.Sprintf("%d kungfu%d master%d", i+1, i, i); got != want {
				t.Errorf("got %s; want %s", got, want)
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

	{
		rows, err := db.Query("select lastname, firstname from persons")
		if err != nil {
			t.Fatal(err)
		}

		var got []map[string]string
		err = Unmarshal(rows, &got)
		if err != nil {
			t.Error(err)
		}

		want := []map[string]string{
			{"firstname": "kungfu0", "lastname": "master0"},
			{"firstname": "kungfu1", "lastname": "master1"},
		}

		if r := reportErrIfNotEqual(t, got, want); r != "" {
			t.Error(r)
		}
	}
	{
		rows, err := db.Query("select lastname 'text.1.lastname', firstname 'text.1.2.firstname' from persons")
		if err != nil {
			t.Fatal(err)
		}

		var got []map[string]interface{}
		err = Unmarshal(rows, &got)
		if err != nil {
			t.Error(err)
		}

		want := []map[string]interface{}{
			map[string]interface{}{
				"text": map[string]interface{}{
					"1": map[string]interface{}{
						"2":        map[string]interface{}{"firstname": "kungfu0"},
						"lastname": "master0",
					},
				},
			},
			map[string]interface{}{
				"text": map[string]interface{}{
					"1": map[string]interface{}{
						"2":        map[string]interface{}{"firstname": "kungfu1"},
						"lastname": "master1",
					},
				},
			},
		}

		if r := reportErrIfNotEqual(t, got, want); r != "" {
			t.Error(r)
		}
	}
}

// func TestUnmarshalSliceMapSlice(t *testing.T) {
// 	_, err := db.Exec(`
// 		DROP TABLE IF EXISTS persons;

// 		CREATE TABLE persons(
// 			id 			integer PRIMARY KEY,
// 			lastname 	varchar(255),
// 			firstname 	varchar(255),
// 			sex 		varchar
// 		);

// 		INSERT INTO persons (firstname, lastname, sex) VALUES ("kungfu0", "master0", "female");
// 		INSERT INTO persons (firstname, lastname, sex) VALUES ("kungfu1", "master1", "female");
// 	`)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	var ps []map[string][]string
// 	err = Unmarshal(rows, &ps)
// 	if err != nil {
// 		t.Error(err)
// 	}

// 	if len(ps) != 2 {
// 		t.Errorf("len(ps) = %d; want 2", len(ps))
// 	}
// 	for i, p := range ps {
// 		key := fmt.Sprintf("master%d", i)
// 		if got, ok := p[key]; ok {
// 			want := fmt.Sprintf("kungfu%d female", i)
// 			if g := strings.Join(got, " "); g != want {
// 				t.Errorf(`ps[%q] %q; want %q`, key, g, want)
// 			}
// 		} else {
// 			t.Errorf("ps[%s] do not exist", key)
// 		}
// 	}
// }

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

	{
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
	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string]*string{}
		err = Unmarshal(rows, &ps)
		if err != nil {
			t.Error(err)
		}

		if got, ok := ps["master0"]; ok {
			if *got != "kungfu0" {
				t.Errorf(`ps["master0"] %q; want "kungfu0"`, got)
			}
		} else {
			t.Error(`ps["master0"] does not exist`)
		}
		if got, ok := ps["master1"]; ok {
			if *got != "kungfu1" {
				t.Errorf(`ps["master1"] %q; want "kungfu1"`, got)
			}
		} else {
			t.Error(`ps["master1"] does not exist`)
		}
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

	{
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
	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string]*[]string{}
		err = Unmarshal(rows, &ps)
		if err != nil {
			t.Error(err)
		}

		if got, ok := ps["master0"]; ok {
			if g := strings.Join(*got, " "); g != "kungfu0 male" {
				t.Errorf(`ps["master0"] %q; want "kungfu0 male"`, g)
			}
		} else {
			t.Error(`ps["master0"] does not exist`)
		}
		if got, ok := ps["master1"]; ok {
			if g := strings.Join(*got, " "); g != "kungfu1 female" {
				t.Errorf(`ps["master1"] %q; want "kungfu1 female"`, g)
			}
		} else {
			t.Error(`ps["master1"] does not exist`)
		}
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

	{
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
	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string]*struct {
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
}

func TestUnmarshalMapKeyStruct(t *testing.T) {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id 			integer PRIMARY KEY,
			lastname 	varchar(255),
			dream	 	varchar(255),
			firstname 	varchar(255),
			sex 		varchar
		);

		INSERT INTO persons (firstname, lastname, dream, sex) VALUES ("kungfu0", "master0", "master", "male");
		INSERT INTO persons (firstname, lastname, dream, sex) VALUES ("kungfu1", "master1", "master", "female");
	`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select lastname as 'sqlmapkey.lastname', dream as 'sqlmapkey.dream', firstname, sex from persons")
	if err != nil {
		t.Fatal(err)
	}

	type key struct{ LastName, Dream string }
	ps := map[key]struct {
		FirstName string
		Sex       string
	}{}
	err = Unmarshal(rows, &ps)
	if err != nil {
		t.Error(err)
	}

	if got, ok := ps[key{"master0", "master"}]; ok {
		if g := got.FirstName + " " + got.Sex; g != "kungfu0 male" {
			t.Errorf(`ps["master0"] %q; want "kungfu0 male"`, g)
		}
	} else {
		t.Error(`ps["master0"] does not exist`)
	}
	if got, ok := ps[key{"master1", "master"}]; ok {
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

	{
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
	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		var ps map[string]*map[string]string
		err = Unmarshal(rows, &ps)
		if err != nil {
			t.Error(err)
		}

		if got, ok := ps["master0"]; ok {
			if g := (*got)["firstname"] + " " + (*got)["sex"]; g != "kungfu0 male" {
				t.Errorf(`ps["master0"] %q; want "kungfu0 male"`, g)
			}
		} else {
			t.Error(`ps["master0"] does not exist`)
		}
		if got, ok := ps["master1"]; ok {
			if g := (*got)["firstname"] + " " + (*got)["sex"]; g != "kungfu1 female" {
				t.Errorf(`ps["master1"] %q; want "kungfu1 female"`, g)
			}
		} else {
			t.Error(`ps["master1"] does not exist`)
		}
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

	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string][]*struct {
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
	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string]*[]struct {
			FirstName string
			Sex       string
		}{}
		err = Unmarshal(rows, &ps)
		if err != nil {
			t.Error(err)
		}

		if got, ok := ps["master0"]; ok {
			if len(*got) != 2 {
				t.Errorf(`len(ps["master0"]) = %d; want 2`, len(*got))
			}
			for i, p := range *got {
				want := fmt.Sprintf("kungfu0-%d male", i)
				if gotstr := p.FirstName + " " + p.Sex; gotstr != want {
					t.Errorf(`ps["master0"][%d] = %q; want %q`, i, gotstr, want)
				}
			}
		} else {
			t.Error(`ps["master0"] does not exist`)
		}
		if got, ok := ps["master1"]; ok {
			if len(*got) != 1 {
				t.Errorf(`len(ps["master1"]) = %d; want 1`, len(*got))
			}
			for i, p := range *got {
				want := "kungfu1 female"
				if gotstr := p.FirstName + " " + p.Sex; gotstr != want {
					t.Errorf(`ps["master1"][%d] = %q; want %q`, i, gotstr, want)
				}
			}
		} else {
			t.Error(`ps["master1"] does not exist`)
		}
	}
}

func TestUnmarshalMapArrayStruct(t *testing.T) {
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

	type data struct {
		FirstName string
		Sex       string
	}
	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string][2]*data{}
		err = Unmarshal(rows, &ps)
		if err != nil {
			t.Error(err)
		}

		want := map[string][2]*data{
			"master0": [2]*data{
				&data{
					FirstName: "kungfu0-0",
					Sex:       "male",
				},
				&data{
					FirstName: "kungfu0-1",
					Sex:       "male",
				},
			},
			"master1": [2]*data{
				&data{
					FirstName: "kungfu1",
					Sex:       "female",
				},
				nil,
			},
		}
		reportErrIfNotEqual(t, ps, want)
	}
	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string]*[2]data{}
		err = Unmarshal(rows, &ps)
		if err != nil {
			t.Error(err)
		}

		want := map[string]*[2]data{
			"master0": &[2]data{
				data{
					FirstName: "kungfu0-0",
					Sex:       "male",
				},
				data{
					FirstName: "kungfu0-1",
					Sex:       "male",
				},
			},
			"master1": &[2]data{
				data{
					FirstName: "kungfu1",
					Sex:       "female",
				},
				data{},
			},
		}
		reportErrIfNotEqual(t, ps, want)
	}
}

func TestUnmarshalMapSliceMap(t *testing.T) {
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

	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string][]map[string]string{}
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
				if gotstr := p["firstname"] + " " + p["sex"]; gotstr != want {
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
				if gotstr := p["firstname"] + " " + p["sex"]; gotstr != want {
					t.Errorf(`ps["master1"][%d] = %q; want %q`, i, gotstr, want)
				}
			}
		} else {
			t.Error(`ps["master1"] does not exist`)
		}
	}
	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string][]*map[string]string{}
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
				if gotstr := (*p)["firstname"] + " " + (*p)["sex"]; gotstr != want {
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
				if gotstr := (*p)["firstname"] + " " + (*p)["sex"]; gotstr != want {
					t.Errorf(`ps["master1"][%d] = %q; want %q`, i, gotstr, want)
				}
			}
		} else {
			t.Error(`ps["master1"] does not exist`)
		}
	}
}

func TestUnmarshalMapSliceSlice(t *testing.T) {
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

	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string][][]string{}
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
				if gotstr := strings.Join(p, " "); gotstr != want {
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
				if gotstr := strings.Join(p, " "); gotstr != want {
					t.Errorf(`ps["master1"][%d] = %q; want %q`, i, gotstr, want)
				}
			}
		} else {
			t.Error(`ps["master1"] does not exist`)
		}
	}
	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string][]*[]string{}
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
				if gotstr := strings.Join(*p, " "); gotstr != want {
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
				if gotstr := strings.Join(*p, " "); gotstr != want {
					t.Errorf(`ps["master1"][%d] = %q; want %q`, i, gotstr, want)
				}
			}
		} else {
			t.Error(`ps["master1"] does not exist`)
		}
	}
}

func TestUnmarshalMapSliceArray(t *testing.T) {
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

	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string][][2]string{}
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
				if gotstr := strings.Join(p[:], " "); gotstr != want {
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
				if gotstr := strings.Join(p[:], " "); gotstr != want {
					t.Errorf(`ps["master1"][%d] = %q; want %q`, i, gotstr, want)
				}
			}
		} else {
			t.Error(`ps["master1"] does not exist`)
		}
	}
	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string][]*[2]string{}
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
				if gotstr := strings.Join((*p)[:], " "); gotstr != want {
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
				if gotstr := strings.Join((*p)[:], " "); gotstr != want {
					t.Errorf(`ps["master1"][%d] = %q; want %q`, i, gotstr, want)
				}
			}
		} else {
			t.Error(`ps["master1"] does not exist`)
		}
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

	{
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
	{
		rows, err := db.Query("select lastname as sqlmapkey, firstname, sex from persons")
		if err != nil {
			t.Fatal(err)
		}

		ps := map[string]*[2]string{}
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
