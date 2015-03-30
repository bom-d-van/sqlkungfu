package sqlkungfu

import (
	"crypto/md5"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"time"
)

type Migration struct {
	Checksum  string
	CreatedAt time.Time
}

type Date time.Time

func (d *Date) Scan(value interface{}) (err error) {
	*d = Date(value.(time.Time))
	return
}

func (d Date) Value() (v driver.Value, err error) {
	v = time.Time(d).Format("2006-01-02")
	return
}

func (d Date) String() string {
	return time.Time(d).Format("2006-01-02")
}

func InitMigration(db *sql.DB) (err error) {
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS sqlkungfu_migrations (
			checksum  varchar(255) NOT NULL Primary Key,
			createdat date
		);
	`)

	return
}

func Exec(db *sql.DB, query string, args ...interface{}) (s Migration, r sql.Result, err error) {
	sum := fmt.Sprintf("%x", md5.Sum([]byte(query)))
	row := db.QueryRow("select * from sqlkungfu_migrations where checksum = ?", sum)
	if err = row.Scan(&s.Checksum, &s.CreatedAt); err == nil {
		return
	} else if err != sql.ErrNoRows {
		return
	}

	if r, err = db.Exec(query, args...); err != nil {
		return
	}

	s.Checksum = sum
	// d := Date(time.Now().Truncate(time.Hour * 24))
	s.CreatedAt = time.Now()
	_, _, err = Insert(db, s, TableName("sqlkungfu_migrations"))

	return
}
