package main

import (
	"arrr/internal/avro"
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/arrow/go/v13/parquet"
	pq "github.com/apache/arrow/go/v13/parquet/pqarrow"
)

func main() {
	ts := time.Now()
	filepath := "./imp.avro"
	info, err := os.Stat(filepath)
	if err != nil {
		bail(err)
	}
	filesize := info.Size()
	fh, err := os.Open(filepath) // file is stored in fh
	if err != nil {
		bail(err)
	}
	fmt.Printf("file : %v\nsize: %v MB\n", filepath, float64(filesize)/1024/1024)
	ior := bufio.NewReader(fh)
	arrReader := avro.NewOCFReader(ior, avro.WithChunk(-1))
	//fmt.Println("tlrname", arrReader.TLRName())
	arrReader.Next()
	recs := arrReader.Record()

	fmt.Printf("File rows: %v  col: %v\n", recs.NumRows(), recs.NumCols())
	f, err := os.OpenFile("./test.parquet", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		bail(err)
	}
	defer f.Close()
	pwProperties := parquet.NewWriterProperties(parquet.WithDictionaryDefault(true), parquet.WithVersion(2))
	awProperties := pq.NewArrowWriterProperties(pq.WithStoreSchema())
	pr, err := pq.NewFileWriter(arrReader.Schema(), f, pwProperties, awProperties)
	if err != nil {
		bail(err)
	}
	defer pr.Close()
	fmt.Printf("parquet version: %v\n", pwProperties.Version())

	err = pr.Write(recs)
	if err != nil {
		panic(err)
	}
	pr.Close()
	log.Printf("time to convert: %v\n", time.Since(ts))
}

func bail(err error) {
	panic(err)
	fmt.Fprintf(os.Stderr, "%s\n", err)
	os.Exit(1)
}

func GetValue(m map[string]interface{}, keys []string) interface{} {
	var value interface{} = m
	for _, key := range keys {
		valueMap, ok := value.(map[string]interface{})
		if !ok {
			return nil
		}
		value, ok = valueMap[key]
		if !ok {
			return nil
		}
	}
	return value
}

func GetValueG[T any](m map[string]T, keys []string) (T, bool) {

	var value T
	for _, key := range keys {
		valueMap, ok := m[key]
		if !ok {
			return *new(T), false
		}

		value = valueMap
	}

	return value, true
}
