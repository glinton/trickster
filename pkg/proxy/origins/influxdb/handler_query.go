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

package influxdb

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tricksterproxy/trickster/pkg/proxy/engines"
	"github.com/tricksterproxy/trickster/pkg/proxy/errors"
	"github.com/tricksterproxy/trickster/pkg/proxy/params"
	"github.com/tricksterproxy/trickster/pkg/proxy/request"
	"github.com/tricksterproxy/trickster/pkg/proxy/timeconv"
	"github.com/tricksterproxy/trickster/pkg/proxy/urls"
	"github.com/tricksterproxy/trickster/pkg/timeseries"
	"github.com/tricksterproxy/trickster/pkg/util/regexp/matching"
)

type contextKey int

const (
	showKey contextKey = iota
)

// QueryHandler handles timeseries requests for InfluxDB and processes them through the delta proxy cache
func (c *Client) QueryHandler(w http.ResponseWriter, r *http.Request) {
	_, s, _ := params.GetRequestValues(r)

	s = strings.Replace(strings.ToLower(s), "%20", "+", -1)

	showQuery := !((!strings.HasPrefix(s, "q=show+")) && (!(strings.Index(s, "&q=show+") > 0)))

	// if it's not a show or select statement, just proxy it instead
	if !showQuery && (!strings.HasPrefix(s, "q=select+")) && (!(strings.Index(s, "&q=select+") > 0)) {
		c.ProxyHandler(w, r)
		return
	}

	if showQuery {
		r = r.WithContext(context.WithValue(r.Context(), showKey, true))
		r.Header.Del("Pragma")
		// r.Header.Del("Cache-Control")
		r.Header.Set("Cache-Control", "max-age=60")
		resources := request.GetResources(r)
		if resources != nil {
			if resources.OriginConfig != nil {
				// resources.OriginConfig.TimeseriesRetention = time.Second
				// resources.OriginConfig.TimeseriesTTL = time.Minute
				resources.OriginConfig.TimeseriesTTLSecs = 60
				// resources.OriginConfig.ValueRetention = time.Second
				resources.OriginConfig.TimeseriesRetentionFactor = 1
				resources.OriginConfig.RevalidationFactor = 1
				resources.OriginConfig.MaxTTLSecs = 60
				request.SetResources(r, resources)
			}
		}
		// r.Header.Set("Max-Age", "1m")
	}

	r.URL = urls.BuildUpstreamURL(r, c.baseUpstreamURL)
	engines.DeltaProxyCacheRequest(w, r)
}

// ParseTimeRangeQuery parses the key parts of a TimeRangeQuery from the inbound HTTP Request
func (c *Client) ParseTimeRangeQuery(r *http.Request) (*timeseries.TimeRangeQuery, error) {
	trq := &timeseries.TimeRangeQuery{Extent: timeseries.Extent{}}

	v, _, _ := params.GetRequestValues(r)
	trq.Statement = v.Get(upQuery)
	if trq.Statement == "" {
		return nil, errors.MissingURLParam(upQuery)
	}

	// if the Step wasn't found in the query (e.g., "group by time(1m)"), just proxy it instead
	step, found := matching.GetNamedMatch("step", reStep, trq.Statement)
	if !found {
		step = "1s"
		// if v, ok := r.Context().Value(showKey).(bool); ok && v {
		// 	step = "60s"
		// }
	}

	stepDuration, err := timeconv.ParseDuration(step)
	if err != nil {
		return nil, errors.ErrStepParse
	}
	trq.Step = stepDuration
	trq.Statement, trq.Extent = getQueryParts(trq.Statement)
	if v, ok := r.Context().Value(showKey).(bool); ok && v {
		now := time.Now().Truncate(time.Minute)
		trq.Extent = timeseries.Extent{Start: now, End: now}
	}

	trq.TemplateURL = urls.Clone(r.URL)

	qt := url.Values(http.Header(v).Clone())
	qt.Set(upQuery, trq.Statement)
	// Swap in the Tokenzed Query in the Url Params
	trq.TemplateURL.RawQuery = qt.Encode()

	return trq, nil
}
