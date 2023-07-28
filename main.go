package main

import (
	"arrr/internal/avro"
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
)

func main() {

	fh, err := os.Open("./1k.variants.avro") // file is stored in fh
	if err != nil {
		bail(err)
	}
	ior := bufio.NewReader(fh)
	arrReader := avro.NewOCFReader(ior) //, avro.WithChunk(-1))
	// for _, fb := range arrReader.Bld.Fields() {
	// 	fmt.Printf("Field: %+v\n", fb.Type().String())
	// }
	fmt.Println("tlrname", arrReader.TLRName())
	arrReader.Next()

	//fmt.Printf("%+v\n", arrReader.Record())
	recs := arrReader.Record()
	fmt.Printf("Record %+v\n", recs)
	itr, err := array.NewRecordReader(arrReader.Schema(), []arrow.Record{recs})
	if err != nil {
		log.Fatal(err)
	}
	defer itr.Release()

	n := 0
	for itr.Next() {
		rec := itr.Record()
		fmt.Printf("NumRows %+v\n", rec.NumRows())
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
		if n > 5 {
			break
		}
	}

}

func bail(err error) {
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
