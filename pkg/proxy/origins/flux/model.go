/*
 * Copyright 2018 Comcast Cable Communications Management, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flux

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/tricksterproxy/trickster/pkg/sort/times"
	"github.com/tricksterproxy/trickster/pkg/timeseries"
	str "github.com/tricksterproxy/trickster/pkg/util/strings"

	"github.com/influxdata/influxdb/models"
)

// SeriesEnvelope represents a response object from the Flux HTTP API
type SeriesEnvelope struct {
	Results      FluxCSV               `json:"results"`
	Err          string                `json:"error,omitempty"`
	StepDuration time.Duration         `json:"step,omitempty"`
	ExtentList   timeseries.ExtentList `json:"extents,omitempty"`

	timestamps map[time.Time]bool // tracks unique timestamps in the matrix data
	tsIndex    int                // which colum represents the timestamp
	tslist     times.Times
	isSorted   bool // tracks if the matrix data is currently sorted
	isCounted  bool // tracks if timestamps slice is up-to-date

	updateLock sync.Mutex
}

// Result represents a Result returned from the Flux HTTP API
type Result struct {
	StatementID int          `json:"statement_id"`
	Series      []models.Row `json:"series,omitempty"`
	Err         string       `json:"error,omitempty"`
}

type FluxCSV struct {
	Annotations [][]string
	Columns     []string
	Records     [][]string
}

// MarshalTimeseries converts a Timeseries into a JSON blob
func (c Client) MarshalTimeseries(ts timeseries.Timeseries) ([]byte, error) {
	// Marshal the Envelope back to a json object for Cache Storage
	se, ok := ts.(*SeriesEnvelope)
	if !ok {
		return nil, fmt.Errorf("wanted flux csv, got '%T'", ts)
	}

	bw := &bytes.Buffer{}
	r := csv.NewWriter(bw)

	r.WriteAll(se.Results.Annotations)

	// write the column headers
	r.Write(se.Results.Columns)
	r.WriteAll(se.Results.Records)

	return bw.Bytes(), nil
}

// UnmarshalTimeseries converts a JSON blob into a Timeseries
func (c Client) UnmarshalTimeseries(data []byte) (timeseries.Timeseries, error) {
	var (
		err         error
		r           = csv.NewReader(bytes.NewReader(data))
		annotations = make([][]string, 3)
	)

	for i := range annotations {
		annotations[i], err = r.Read()
		if err != nil && err != io.EOF {
			return nil, err
		}
	}

	heading, err := r.Read()
	if err != nil && err != io.EOF {
		return nil, err
	}

	// todo: this should be moved out for handling schema caching (`buckets()`, etc...)
	tsI := str.IndexOfString(heading, "_time")
	if tsI < 0 {
		return nil, errors.New("timestamp not found in output")
	}

	datas, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	return &SeriesEnvelope{
		Results: FluxCSV{
			Annotations: annotations,
			Columns:     heading,
			Records:     datas,
		},
		tsIndex: tsI,
	}, nil
}

type Extern struct {
	Body *Body `json:"body,omitempty"`
}

type Body struct {
	Assignment *Assignment `json:"assignment,omitempty"`
}

type Assignment struct {
	Init *Init `json:"init,omitempty"`
}

type Init struct {
	Properties []Properties `json:"properties,omitempty"`
}

type Key struct {
	Type string
	Name string
}

type Argument struct {
	Type   string // if type == "DurationLiteral", LOOP duration += values[k]
	Values []map[string]interface{}
}

type Callee struct {
	Type string
	Name string
}

type Value struct {
	Type     string // it type == "UnaryExpression", prepend operator to following duration
	Value    string
	Argument Argument `json:"argument,omitempty"`
	Callee   Callee
}

type Properties struct {
	Type  string
	Key   Key
	Value Value
}

type Dialect struct {
	Annotations []string `json:"annotations,omitempty"`
}
