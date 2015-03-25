package sqlkungfu

import "testing"

func TestInsert(t *testing.T) {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id integer,
			last varchar(255),
			first varchar(255),
			age integer,
			addr string,
			addrii string,

			PRIMARY KEY (id)
		);
	`)
	if err != nil {
		t.Fatal(err)
	}
	type AddressII struct{ AddrII string }
	type Person struct {
		Id        uint64
		FirstName string    `sqlkungfu:"first"`
		LastName  ***string `sqlkungfu:"last"`
		Dream     Dream
		Projects  []Project
		Age       Int
		Address
		AddressII AddressII `sqlkungfu:",inline"`
	}

	var want Person
	want.Age = 20
	last := "Tang"
	lastp := &last
	lastpp := &lastp
	want.LastName = &lastpp
	want.FirstName = "Faye"
	want.Addr = "Netherland"
	want.AddressII.AddrII = "New Zealand"
	insert, _, err := Insert(db, &want)
	if err != nil {
		t.Error(err)
	}

	if want := "INSERT INTO persons (`first`,`last`,`age`,`addr`,`addrii`) VALUES (?,?,?,?,?)"; insert != want {
		t.Errorf("got %q; want %q", insert, want)
	}

	var got Person
	rows, err := db.Query(`select * from persons`)
	if err != nil {
		t.Error(err)
	}
	if err = Unmarshal(rows, &got); err != nil {
		t.Error(err)
	}
	reportErrIfNotEqual(t, got, want)
}

func TestInsertMap(t *testing.T) {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS persons;

		CREATE TABLE persons(
			id integer,
			last varchar(255),
			first varchar(255),
			age integer,
			addr string,
			addrii string,

			PRIMARY KEY (id)
		);
	`)
	if err != nil {
		t.Fatal(err)
	}
	type Person map[string]interface{}
	want := Person{
		"last":  "Lee",
		"first": "Bruce",
	}
	_, _, err = Insert(db, &want)
	if err != nil {
		t.Error(err)
	}

	want["id"] = int64(1)
	var got Person
	rows, err := db.Query(`select id, last, first from persons`)
	if err != nil {
		t.Error(err)
	}
	if err = Unmarshal(rows, &got); err != nil {
		t.Error(err)
	}
	reportErrIfNotEqual(t, got, want)
}
