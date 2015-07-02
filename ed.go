package main

import (
	"fmt"
	"github.com/haylingsailor/ed/db"
	"math/rand"
	_ "runtime"
	"time"
)

var names = []string{
	"Andy",
	"Jim",
	"Sue",
	"SueSpoon",
}

func sessionUpdater(instanceNum int, numIterations int, edDb *db.EdDb, c chan string) {

	for i := 0; i < numIterations; i++ {
		edDb.RecordSessionActivity(0)
	}
	c <- fmt.Sprintf("sessionUpdater %d finished", instanceNum)
}

func personPutter(instanceNum int, numIterations int, edDb *db.EdDb, c chan string) {

	for i := 0; i < numIterations; i++ {
		which := rand.Intn(4)
		edDb.UpsertPerson(which, names[which])
	}
	c <- fmt.Sprintf("dbPutter %d finished", instanceNum)
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	//runtime.GOMAXPROCS(1)
	//runtime.GOMAXPROCS(8)
	fmt.Println("Test start")

	edDb, err := db.New("diskDb.db")

	if err != nil {
		fmt.Println(err)
	} else {
		defer edDb.Close()
	}

	c := make(chan string)
	numPutters := 5
	numSessionUpdaters := 5
	numPutterIter := 100
	numSessionIter := 100000

	for p := 0; p < numPutters; p++ {
		go personPutter(p, numPutterIter, edDb, c)
	}

	for p := 0; p < numSessionUpdaters; p++ {
		go sessionUpdater(p, numSessionIter, edDb, c)
	}

	// We know we've finished when all the putters and sessionUpdaters have
	// sent us a message
	for p := 0; p < numPutters+numSessionUpdaters; p++ {
		fmt.Println(<-c)
	}

	edDb.PrintSessionActivity()

	fmt.Printf("Finished!.\n")

}
