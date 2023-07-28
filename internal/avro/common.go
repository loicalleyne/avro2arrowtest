// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package avro

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/memory"
)

var (
	ErrMismatchFields = errors.New("arrow/avro: number of records mismatch")
)

// Option configures an Avro reader/writer.
type Option func(config)
type config interface{}

// WithAllocator specifies the Arrow memory allocator used while building records.
func WithAllocator(mem memory.Allocator) Option {
	return func(cfg config) {
		switch cfg := cfg.(type) {
		case *OCFReader:
			cfg.mem = mem
		default:
			panic(fmt.Errorf("arrow/avro: unknown config type %T", cfg))
		}
	}
}

// WithChunk specifies the chunk size used while reading Avro OCF files.
//
// If n is zero or 1, no chunking will take place and the reader will create
// one record per row.
// If n is greater than 1, chunks of n rows will be read.
// If n is negative, the reader will load the whole Avro OCF file into memory and
// create one big record with all the rows.
func WithChunk(n int) Option {
	return func(cfg config) {
		switch cfg := cfg.(type) {
		case *OCFReader:
			cfg.chunk = n
		default:
			panic(fmt.Errorf("arrow/avro: unknown config type %T", cfg))
		}
	}
}

func validate(schema *arrow.Schema) {
	for i, f := range schema.Fields() {
		switch ft := f.Type.(type) {
		case *arrow.BooleanType:
		case *arrow.Int32Type, *arrow.Int64Type:
		case *arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type:
		case *arrow.Float32Type, *arrow.Float64Type:
		case *arrow.StringType:
		case *arrow.TimestampType:
		case *arrow.Date32Type:
		case *arrow.Decimal128Type:
		case *arrow.ListType, *arrow.FixedSizeListType:
		case *arrow.BinaryType, *arrow.FixedSizeBinaryType:
		case *arrow.NullType:
		default:
			panic(fmt.Errorf("arrow/csv: field %d (%s) has invalid data type %T", i, f.Name, ft))
		}
	}
}
