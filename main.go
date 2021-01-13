package main

import (
	"bufio"
	"context"
	b64 "encoding/base64"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/jackc/pgx"
)

func process_row(row []string) ([]interface{}, error) {
	result := make([]interface{}, len(row))
	height, _ := strconv.ParseInt(row[0], 10, 64)
	result[0] = height

	cid := row[1]
	result[1] = cid

	from := row[2]
	result[2] = from

	to := row[3]
	result[3] = to

	value := row[4]
	result[4] = value

	gas_fee_cap := row[5]
	result[5] = gas_fee_cap

	gas_premium := row[6]
	result[6] = gas_premium

	gas_limit, _ := strconv.ParseInt(row[7], 10, 64)
	result[7] = gas_limit

	size_bytes, _ := strconv.ParseInt(row[8], 10, 64)
	result[8] = size_bytes

	nonce, _ := strconv.ParseInt(row[9], 10, 64)
	result[9] = nonce

	method, _ := strconv.ParseInt(row[10], 10, 64)
	result[10] = method

	params, _ := b64.StdEncoding.DecodeString(row[11])
	result[11] = params
	return result, nil

}

func worker(filenameChan <-chan string, dbcopyChan chan []interface{}, errlog *log.Logger) {
	defer wg.Done()
	errlog.Println("STARTING")
	for filename := range filenameChan {
		fmt.Println(filename)
		csvFile, err := os.Open(filename)
		if err != nil {
			errlog.Println(err)
			continue
		}
		defer csvFile.Close()
		reader := csv.NewReader(bufio.NewReader(csvFile))
		for {
			row, error := reader.Read()
			if error == io.EOF {
				break
			} else if error != nil {
				continue
			}
			output, err := process_row(row)
			if err != nil {
				fmt.Println(err)
			}
			if err == nil {
				dbcopyChan <- output
			}
		}
	}
}

func dbcopier(ctx context.Context, dbcopyChan chan []interface{}) {
	fmt.Println("STARTING")
	dbc := DBCopier{dbcopyChan, false, 0, nil}

	config := pgx.ConnConfig{
		Host:     "localhost",
		User:     "postgres",
		Database: "postgres",
		Password: "password",
	}
	conn, err := pgx.Connect(config)
	if err != nil {
		fmt.Println("WTF")
		log.Fatal(err)
	}
	copyCount, err := conn.CopyFrom(pgx.Identifier{"messages"},
		[]string{
			"height",
			"cid",
			"from",
			"to",
			"value",
			"gas_fee_cap",
			"gas_premium",
			"gas_limit",
			"size_bytes",
			"nonce",
			"method",
			"params",
		}, &dbc)
	if err != nil {
		fmt.Println(err)
		// log.Fatal(err)
	}
	fmt.Println("Wrote ", copyCount)
	return
	// close(dbcopyChan)
}

type DBCopier struct {
	c        chan []interface{}
	finished bool
	count    int
	current  []interface{}
}

func (dbc *DBCopier) Next() bool {
	if dbc.finished {
		return false
	}
	next := <-dbc.c
	dbc.count += 1
	// fmt.Println(dbc.count)
	if dbc.count > 1000000 {
		dbc.finished = true
	}
	dbc.current = next
	return true

}

func (dbc *DBCopier) Values() ([]interface{}, error) {
	return dbc.current, nil
}

func (dbc DBCopier) Err() error {
	return nil // should probs do this properly..
}

var wg sync.WaitGroup

func main() {
	ctx := context.Background()
	err_file, err := os.OpenFile("err.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}

	errlog := log.New(err_file, "", log.Ldate)

	csvFile, err := os.Open("/home/pl/sentinel-visor/tipsets.csv")
	if err != nil {
		log.Fatal(err)
	}
	reader := csv.NewReader(bufio.NewReader(csvFile))
	filenameChan := make(chan string, 1000)
	dbcopyChan := make(chan []interface{}, 10000)
	workerCount := 20
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(filenameChan, dbcopyChan, errlog)
	}
	go dbcopier(ctx, dbcopyChan)
	for {
		row, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			break
			// log.Fatal(error)
		}
		height := row[0]
		messageFolder := "/lotus/output"
		filename := messageFolder + "/messages" + height + ".csv"
		filenameChan <- filename
	}
	close(filenameChan)
	wg.Wait()
	// time.Sleep(15 * time.Second)
	// close(dbcopyChan)
	// time.Sleep(15 * time.Second)
	return
}
