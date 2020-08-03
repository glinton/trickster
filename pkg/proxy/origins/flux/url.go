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
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/tricksterproxy/trickster/pkg/timeseries"
)

// Upstream Endpoints
const (
	mnQuery = "api/v2/query"
)

// Common URL Parameter Names
const (
	upQuery = "q"
	upDB    = "db"
	org     = "org"
	orgID   = "orgID"
)

var frKey = fluxReq{}

// SetExtent will change the upstream request query to use the provided Extent
func (c Client) SetExtent(r *http.Request, trq *timeseries.TimeRangeQuery, extent *timeseries.Extent) {
	interQuery := interpolateTimeQuery(trq.Statement, extent)

	dat, _ := json.Marshal(&fluxReq{
		Query: interQuery,
		Dialect: &Dialect{
			Annotations: []string{"datatype", "group", "default"},
		},
	})
	r.ContentLength = int64(len(dat))
	r.Body = ioutil.NopCloser(bytes.NewBuffer(dat))
}
