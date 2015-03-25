# sqlkungfu

sqlkungfu is a very simple data binding package, built along with database/sql package. It doesn't assume fixed bindings between your structs and tables. Instead, it gives the ability to avail yourself of the powerful combination of golang and SQL.

## Unmarshal

### common

```sql
select id, name, phone, sex from persons limit 1
```

```golang
var data struct{
	ID    uint
	Name  string
	Phone string
	Sex   string
}
sqlkung.Marshal(rows, &data)

var data []string
sqlkung.Marshal(rows, &data)

var data [4]string
sqlkung.Marshal(rows, &data)

var data map[string]interface{}
sqlkung.Marshal(rows, &data)
```

```sql
select id, name, phone, sex from persons limit 10
```

```golang
var data []struct{
	ID    uint
	Name  string
	Phone string
	Sex   string
}
sqlkung.Marshal(rows, &data)

var data [][]string
sqlkung.Marshal(rows, &data)

var data [10][4]string
sqlkung.Marshal(rows, &data)

var data []map[string]interface{}
sqlkung.Marshal(rows, &data)
```

### schema

```sql
select num.id, num.age, text.name, text.phone, text.sex from persons limit 10
```

```golang
var data []struct{
	Num struct{
		ID    uint
		Age uint
	}
	Text struct{
		Name  string
		Phone string
		Sex   string
	}
}
sqlkung.Marshal(rows, &data)
```

## Insert/Update

