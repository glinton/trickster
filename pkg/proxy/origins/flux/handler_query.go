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
	"context"
	"encoding/json"
	errs "errors"
	"net/http"
	"strings"

	"github.com/tricksterproxy/trickster/pkg/proxy/engines"
	"github.com/tricksterproxy/trickster/pkg/proxy/errors"
	"github.com/tricksterproxy/trickster/pkg/proxy/timeconv"
	"github.com/tricksterproxy/trickster/pkg/proxy/urls"
	"github.com/tricksterproxy/trickster/pkg/timeseries"
	"github.com/tricksterproxy/trickster/pkg/util/regexp/matching"
)

type fluxReq struct {
	Extern  *Extern  `json:"extern,omitempty"`  // assignment.init.properties.[].value.argument.values.[] are important to populate query. ui could pre-populate flux to remove all this bloat
	Query   string   `json:"query"`             // flux query
	Dialect *Dialect `json:"dialect,omitempty"` // for getting annotations, likely not needed since we hardcode this for the upstream's request
}

func (f *fluxReq) GetQuery() string {
	if f == nil {
		return ""
	}

	return strings.TrimSpace(f.Query)
}

// QueryHandler handles timeseries requests for InfluxDB 2.0 and processes them through the delta proxy cache
func (c *Client) QueryHandler(w http.ResponseWriter, r *http.Request) {
	fr := &fluxReq{}
	err := json.NewDecoder(r.Body).Decode(fr)
	if err != nil {
		c.ProxyHandler(w, r)
		return
	}

	r = r.WithContext(context.WithValue(r.Context(), frKey, fr))

	r.URL = urls.BuildUpstreamURL(r, c.baseUpstreamURL)
	engines.DeltaProxyCacheRequest(w, r)
}

// ParseTimeRangeQuery parses the key parts of a TimeRangeQuery from the inbound HTTP Request
func (c *Client) ParseTimeRangeQuery(r *http.Request) (*timeseries.TimeRangeQuery, error) {
	trq := &timeseries.TimeRangeQuery{Extent: timeseries.Extent{}}

	fr, ok := r.Context().Value(frKey).(*fluxReq)
	if !ok {
		return nil, errs.New("flux request not found")
	}

	trq.Statement = fr.GetQuery()
	if trq.Statement == "" {
		return nil, errors.MissingRequestParam(mnQuery)
	}

	// if the Step wasn't found in the query (e.g., "group by time(1m)"), just proxy it instead
	step, found := matching.GetNamedMatch("step", reStep, trq.Statement)
	if !found {
		step = "1s"
	}

	stepDuration, err := timeconv.ParseDuration(step)
	if err != nil {
		return nil, errors.ErrStepParse
	}

	trq.Step = stepDuration
	trq.Statement, trq.Extent = getQueryParts(trq.Statement)
	trq.TemplateURL = urls.Clone(r.URL)

	return trq, nil
}
