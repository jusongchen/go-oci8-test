package main

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	_ "github.com/mattn/go-oci8"
)

func main() {
	nlsLang := os.Getenv("NLS_LANG")
	if !strings.HasSuffix(nlsLang, "UTF8") {
		i := strings.LastIndex(nlsLang, ".")
		if i < 0 {
			os.Setenv("NLS_LANG", "AMERICAN_AMERICA.AL32UTF8")
		} else {
			nlsLang = nlsLang[:i+1] + "AL32UTF8"
			fmt.Fprintf(os.Stderr, "NLS_LANG error: should be %s, not %s!\n",
				nlsLang, os.Getenv("NLS_LANG"))
		}
	}

	db, err := sql.Open("oci8", getDSN())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	if err = testSelect(db); err != nil {
		fmt.Println(err)
		return
	}

	if err = testI18n(db); err != nil {
		fmt.Println(err)
		return
	}

	if err = testDataTypes(db); err != nil {
		fmt.Println(err)
		return
	}

}

func getDSN() string {
	var dsn string
	if len(os.Args) > 1 {
		dsn = os.Args[1]
		if dsn != "" {
			return dsn
		}
	}
	dsn = os.Getenv("GO_OCI8_CONNECT_STRING")
	if dsn != "" {
		return dsn
	}
	fmt.Fprintln(os.Stderr, `Please specifiy connection parameter in GO_OCI8_CONNECT_STRING environment variable,
or as the first argument! (The format is user/name@host:port/sid)`)
	return "scott/tiger@XE"
}

func testSelect(db *sql.DB) error {
	rows, err := db.Query("select instance_name,host_name,version,startup_time from v$instance  ")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var inst_name, host_name, version string
		var startup_time time.Time

		rows.Scan(&inst_name, &host_name, &version, &startup_time)
		fmt.Println(inst_name, host_name, version, startup_time) // 3.14 foo
	}
	_, err = db.Exec("create table foo(bar varchar2(256))")
	_, err = db.Exec("drop table foo")
	if err != nil {
		return err
	}

	return nil
}

func testI18n(db *sql.DB) error {

	tbl := "tst_oci8_i18n_tmp"
	tst_strings := "'Habitación doble', '雙人房', 'двухместный номер'"

	_, _ = db.Exec("DROP TABLE " + tbl)
	defer db.Exec("DROP TABLE " + tbl)
	if _, err := db.Exec("CREATE TABLE " + tbl + " (name_spainish VARCHAR2(100), name_chinesses VARCHAR2(100), name_russian VARCHAR2(100))"); err != nil {
		return err
	}
	if _, err := db.Exec("INSERT INTO " + tbl +
		" (name_spainish, name_chinesses, name_russian) " +
		" VALUES (" + tst_strings + ")"); err != nil {
		return err
	}

	rows, err := db.Query("select name_spainish, name_chinesses, name_russian from " + tbl)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var nameSpainish string
		var nameChinesses string
		var nameRussian string
		if err = rows.Scan(&nameSpainish, &nameChinesses, &nameRussian); err != nil {
			return err
		}
		got := fmt.Sprintf("'%s', '%s', '%s'", nameSpainish, nameChinesses, nameRussian)
		fmt.Println(got)
		if got != tst_strings {
			return fmt.Errorf("ERROR: string mismatch: got %q, awaited %q\n", got, tst_strings)
		}
	}
	return rows.Err()
}

func testDataTypes(db *sql.DB) error {

	db.Exec("drop table blob_example$tmp")

	_, err := db.Exec(`
		create table blob_example$tmp(
			id varchar2(256) not null primary key
			, data blob
			,raw_val raw(128)
			,date_val date
			)
		`)
	if err != nil {
		return err
	}

	//--raw_val := make([]byte, md5.Size)
	md5_sum := md5.Sum([]byte("These pretzels are making me thirsty."))
	raw_val := md5_sum[:]

	//round time to second as oracle date type does not keep fraction second
	date_val_wanted := time.Now().Round(time.Second)

	// Over 4000bytes
	b := []byte(strings.Repeat(`請在12：15PM左右將每份裝好在盤子裡，可以先裝50份左右以及20份小孩的（分量少一点，避免吃不完浪费）集中摆放在三张桌子上，以免主日学後造成排隊等候。 每份餐装每一樣菜	一勺（现在是5个菜，分菜的勺不要拿太大的）和一大勺米飯（分饭直到剩下最后一大锅米饭`, 100))

	_, err = db.Exec("insert into blob_example$tmp(id, data,raw_val,date_val) values(:1, :2,:3,:4)", "aString", b, raw_val, date_val_wanted)
	if err != nil {
		return err
	}

	rows, err := db.Query("select * from blob_example$tmp")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var data []byte
		var date_val_get time.Time

		raw_value_get := make([]byte, md5.Size)

		rows.Scan(&id, &data, &raw_value_get, &date_val_get)
		if string(b) != string(data) {
			fmt.Printf("\nGet %v Wanted %v", string(data), string(b))
			fmt.Printf("\nDate data type doesn't work correctly")

			panic("BLOB data type doesn't work correctly")
		} else {
			fmt.Printf("\nBLOB data type works")
		}

		if !reflect.DeepEqual(raw_value_get, raw_val) {
			fmt.Printf("\nGet %v Wanted %v", raw_value_get, raw_val)

			panic("RAW data type doesn't work correctly")
		} else {
			fmt.Printf("\nRaw data type works")
		}

		if date_val_get != date_val_wanted {
			fmt.Printf("\nGet %v Wanted %v", date_val_get, date_val_wanted)
			fmt.Printf("\nDate data type doesn't work correctly")
		} else {
			fmt.Printf("\nDate data type works")
		}

	}
	return nil
}
