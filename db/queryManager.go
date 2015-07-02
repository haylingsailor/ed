// Package db defines prepared statements used by the rest of the system
package db

import (
	"database/sql"
	"fmt"
	// sqlx is a superset of go's database/sql
	"github.com/jmoiron/sqlx"
	//sqlite driver for database/sql
	sqlite "github.com/mattn/go-sqlite3"
	"log"
	"time"
)

// EdDb provides database support for ed
type EdDb struct {

	// Database
	dbConn *sqlx.DB

	// Prepared statements: prepared on dbConn but should be usable from memDb
	statements map[string]*sqlx.NamedStmt
}

// New tries to initialise the disk-based database and create memory
func New(dbFilename string) (*EdDb, error) {

	// First, create a hook which will attach a shared memory-only database to
	// each connction opened by golang's database/sql connection pool
	sql.Register("sqlite3ConnectionCatchingDriver",
		&sqlite.SQLiteDriver{
			ConnectHook: func(newConn *sqlite.SQLiteConn) error {
				newConn.Exec("ATTACH DATABASE 'file::memory:?cache=shared&busy_timeout=60000' AS mem", nil)
				fmt.Println("Attach Database to ", newConn)
				return nil
			},
		},
	)

	// The hook is now in place, so can create a connection to the disk Db:
	dbURI := fmt.Sprintf("file:%s?cache=shared&busy_timeout=60000", dbFilename)
	dbConn, err := sqlx.Connect("sqlite3ConnectionCatchingDriver", dbURI)
	if err != nil {
		return nil, err
	}

	dbConn.SetMaxIdleConns(10)
	dbConn.Exec(`PRAGMA foreign_keys = ON;`)

	edDb := EdDb{
		dbConn:     dbConn,
		statements: map[string]*sqlx.NamedStmt{},
	}

	err = edDb.initDbSchema()
	if err != nil {
		dbConn.Close()
		return nil, err
	}

	err = edDb.buildPreparedStatements()
	if err != nil {
		dbConn.Close()
		return nil, err
	}
	return &edDb, nil
}

// Close tidies up everything
func (db *EdDb) Close() {
	// close prepared statements
	for title := range db.preparedStatements() {
		db.statements[title].Close()
	}

	//close databases
	db.dbConn.Close()
	return
}

// UpsertPerson blah
func (db *EdDb) UpsertPerson(id int, name string) (err error) {
	args := map[string]interface{}{
		"id":   id,
		"name": name,
	}
	_, err = db.statements["updatePerson"].Exec(args)
	if err != nil {
		log.Fatal("updatePerson: ", err)
	}
	_, err = db.statements["insertPerson"].Exec(args)
	if err != nil {
		log.Fatal("insertPerson: ", err)
	}
	return
}

// RecordSessionActivity blah
func (db *EdDb) RecordSessionActivity(personID int) (err error) {

	_, err = db.statements["recordSessionActivity"].Exec(map[string]interface{}{
		"personId": personID,
	})

	if err != nil {
		log.Fatal("RecordSessionActivity: ", err)
	}
	return
}

// PrintSessionActivity blah
func (db *EdDb) PrintSessionActivity() (err error) {
	rows, err := db.statements["getSessionActivity"].Query(map[string]interface{}{
		"id":   1,
		"name": "f",
	})
	if err != nil {
		log.Fatal("PrintSessionActivity : ", err)
	}

	// iterate over each row
	for rows.Next() {
		var personName string
		var personID int64
		var dateTime time.Time
		var numItems int64
		err = rows.Scan(&personName, &personID, &dateTime, &numItems)
		fmt.Println("Result:", personID, personName, dateTime, numItems)
	}

	return
}

// initDbSchema initialises both the disk schema and the memory-only database,
// which is created and attached immediately by the ATTACH command below
// IMPORTANT! If you use the 'file::memory:?....' form rather than
//                           'file:mem.db?mode=memory' form, then the memory
// db will NOT be visible from all connections to the db. The file 'mem.db' does
// not get created as long as mode=memory, but all go subroutines (accessing
// via different connections) WILL be able to access the same memory db..

func (db *EdDb) initDbSchema() (err error) {

	// First, the persistent parts of the database (main.), then the
	// ephemeral parts (mem.)
	_, err = db.dbConn.Exec(`

        CREATE TABLE IF NOT EXISTS main.person (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            numUpdates INTEGER DEFAULT 0
        );

        CREATE TABLE mem.sessionActivity (
            id INTEGER PRIMARY KEY,
            personId INTEGER NOT NULL,
            dateTime DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    `)
	return
}

func (db *EdDb) preparedStatements() map[string]string {
	return map[string]string{

		"insertPerson": `

            INSERT OR IGNORE INTO person (id, name)
            VALUES(:id, :name)
            ;
        `,

		"updatePerson": `

            UPDATE person SET
                name=:name,
                numUpdates=numUpdates + 1
            WHERE
                id=:id
            ;
        `,

		"recordSessionActivity": `

           INSERT INTO sessionActivity (personId)
           VALUES(:personId);
        `,

		"getSessionActivity": `

            SELECT
               main.person.name as personName,
               main.person.id as personId,
               mem.sessionActivity.dateTime as dateTime,
               count(*) as numItems
           FROM mem.sessionActivity
           LEFT OUTER JOIN main.person
               ON mem.sessionActivity.personId = main.person.id
           GROUP BY
               main.person.id
           ORDER BY
               mem.sessionActivity.dateTime ASC
        `,
	}
}

// BuildPreparedStatements builds prepared statements
func (db *EdDb) buildPreparedStatements() (err error) {

	for title, sqlCommand := range db.preparedStatements() {
		db.statements[title], err = db.dbConn.PrepareNamed(sqlCommand)
		if err != nil {
			log.Fatal(fmt.Sprint("buildPreparedStatement:", title, " ", err))
			return
		}
	}
	return
}
