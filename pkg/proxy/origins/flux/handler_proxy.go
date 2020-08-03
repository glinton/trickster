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
	"net/http"

	"github.com/tricksterproxy/trickster/pkg/proxy/engines"
	"github.com/tricksterproxy/trickster/pkg/proxy/urls"
)

// ProxyHandler sends a request through the basic reverse proxy to the origin,
// and services non-cacheable InfluxDB 2.0 API calls
func (c *Client) ProxyHandler(w http.ResponseWriter, r *http.Request) {
	r.URL = urls.BuildUpstreamURL(r, c.baseUpstreamURL)
	engines.DoProxy(w, r, true)
}
