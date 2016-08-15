// go-mysqlbinlog: a simple binlog tool to sync remote MySQL binlog.
// go-mysqlbinlog supports semi-sync mode like facebook mysqlbinlog.
// see http://yoshinorimatsunobu.blogspot.com/2014/04/semi-synchronous-replication-at-facebook.html
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

var host = flag.String("host", "127.0.0.1", "MySQL host")
var port = flag.Int("port", 3306, "MySQL port")
var user = flag.String("user", "root", "MySQL user, must have replication privilege")
var password = flag.String("password", "", "MySQL password")

var flavor = flag.String("flavor", "mysql", "Flavor: mysql or mariadb")

var file = flag.String("file", "", "Binlog filename")
var pos = flag.Int("pos", 4, "Binlog position")

var semiSync = flag.Bool("semisync", false, "Support semi sync")
var backupPath = flag.String("backup_path", "", "backup path to store binlog files")

var rawMode = flag.Bool("raw", false, "Use raw mode")

var tableColumnsSQL = "select column_name from columns where table_schema = ? and table_name = ?"

func createDB(user string, password string, host string, port int, name string) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", user, password, host, port, name)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

func closeDB(db *sql.DB) error {
	return errors.Trace(db.Close())
}

func getTableColumns(db *sql.DB, schema string, table string) ([]string, error) {
	if table == "" {
		return nil, errors.New("table name is empty")
	}

	var columns []string
	rows, err := db.Query(tableColumnsSQL, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		var field string
		err = rows.Scan(
			&field,
		)

		if err != nil {
			return nil, errors.Trace(err)
		}

		columns = append(columns, field)
	}

	return columns, nil
}

func genInsertSQLs(schema string, table string, datas [][]interface{}, columns []string) ([]string, error) {
	sqls := make([]string, 0, len(datas))
	columnList := strings.Join(columns, ",")
	for _, data := range datas {
		if len(data) != len(columns) {
			return nil, errors.Errorf("invalid columns and datas - %d, %d", len(datas), len(columns))
		}

		values := make([]string, 0, len(data))
		for _, value := range data {
			values = append(values, fmt.Sprintf("%v", value))
		}

		valueList := strings.Join(values, ",")
		sql := fmt.Sprintf("insert into %s.%s (%s) values (%s);", schema, table, columnList, valueList)
		sqls = append(sqls, sql)
	}

	return sqls, nil
}

func genWhere(columns []string, data []interface{}, split string) string {
	var kvs []byte
	for i := range columns {
		if i == len(columns)-1 {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = %v", columns[i], data[i]))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = %v%s", columns[i], data[i], split))...)
		}
	}

	return string(kvs)
}

func genKVs(columns []string, data []interface{}, split string) string {
	var kvs []byte
	for i := range columns {
		if i == len(columns)-1 {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = %v", columns[i], data[i]))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = %v%s", columns[i], data[i], split))...)
		}
	}

	return string(kvs)
}

func genUpdateSQLs(schema string, table string, datas [][]interface{}, columns []string) ([]string, error) {
	sqls := make([]string, 0, len(datas)/2)
	for i := 0; i < len(datas); i += 2 {
		oldData := datas[i]
		newData := datas[i+1]
		if len(oldData) != len(newData) {
			return nil, errors.Errorf("invalid update datas - %d, %d", len(oldData), len(newData))
		}

		oldValues := make([]string, 0, len(oldData))
		newValues := make([]string, 0, len(newData))
		updateColumns := make([]string, 0, len(columns))

		for j := range oldData {
			if reflect.DeepEqual(oldData[j], newData[j]) {
				continue
			}

			updateColumns = append(updateColumns, columns[j])
			oldValues = append(oldValues, fmt.Sprintf("%v", oldData[j]))
			newValues = append(newValues, fmt.Sprintf("%v", newData[j]))
			kvs := genKVs(updateColumns, newData, ", ")
			where := genWhere(updateColumns, oldData, " and ")
			sql := fmt.Sprintf("update %s.%s set %s where %s;", schema, table, kvs, where)
			sqls = append(sqls, sql)
		}
	}

	return sqls, nil
}

func main() {
	flag.Parse()

	db, err := createDB(*user, *password, *host, *port, "information_schema")
	if err != nil {
		fmt.Printf("create mysql connection failed: %v\n", errors.ErrorStack(err))
		return
	}

	defer closeDB(db)

	b := replication.NewBinlogSyncer(101, *flavor)

	if err := b.RegisterSlave(*host, uint16(*port), *user, *password); err != nil {
		fmt.Printf("Register slave error: %v \n", errors.ErrorStack(err))
		return
	}

	b.SetRawMode(*rawMode)

	if *semiSync {
		if err := b.EnableSemiSync(); err != nil {
			fmt.Printf("Enable semi sync replication mode err: %v\n", errors.ErrorStack(err))
			return
		}
	}

	pos := mysql.Position{*file, uint32(*pos)}
	if len(*backupPath) > 0 {
		// must raw mode
		b.SetRawMode(true)

		err := b.StartBackup(*backupPath, pos, 0)
		if err != nil {
			fmt.Printf("Start backup error: %v\n", errors.ErrorStack(err))
			return
		}
	} else {
		s, err := b.StartSync(pos)
		if err != nil {
			fmt.Printf("Start sync error: %v\n", errors.ErrorStack(err))
			return
		}

		for {
			e, err := s.GetEvent()
			if err != nil {
				fmt.Printf("Get event error: %v\n", errors.ErrorStack(err))
				return
			}

			switch ev := e.Event.(type) {
			case *replication.RowsEvent:
				switch e.Header.EventType {
				case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					schema := string(ev.Table.Schema)
					table := string(ev.Table.Table)
					columns, err := getTableColumns(db, schema, table)
					if err != nil {
						fmt.Printf("parse rows event failed: %v\n", errors.ErrorStack(err))
						return
					}

					sqls, err := genInsertSQLs(schema, table, ev.Rows, columns)
					if err != nil {
						fmt.Printf("gen insert sqls failed: %v\n", errors.ErrorStack(err))
						return
					}

					for i, sql := range sqls {
						fmt.Printf("[insert]%d - %s\n", i, sql)
					}
				case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					schema := string(ev.Table.Schema)
					table := string(ev.Table.Table)
					columns, err := getTableColumns(db, schema, table)
					if err != nil {
						fmt.Printf("parse rows event failed: %v\n", errors.ErrorStack(err))
						return
					}

					sqls, err := genUpdateSQLs(schema, table, ev.Rows, columns)
					if err != nil {
						fmt.Printf("gen insert sqls failed: %v\n", errors.ErrorStack(err))
						return
					}

					for i, sql := range sqls {
						fmt.Printf("[update]%d - %s\n", i, sql)
					}
				}
			}

			e.Dump(os.Stdout)
		}
	}
}
