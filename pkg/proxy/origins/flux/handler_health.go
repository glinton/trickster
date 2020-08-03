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
	"net/http"

	tctx "github.com/tricksterproxy/trickster/pkg/proxy/context"
	"github.com/tricksterproxy/trickster/pkg/proxy/engines"
	"github.com/tricksterproxy/trickster/pkg/proxy/request"
	"github.com/tricksterproxy/trickster/pkg/proxy/urls"
)

// HealthHandler checks the health of the Configured Upstream Origin
func (c *Client) HealthHandler(w http.ResponseWriter, r *http.Request) {
	if c.healthURL == nil {
		c.populateHeathCheckRequestValues()
	}

	// seems this could never be true, maybe `!= GET`?
	if c.healthMethod == "-" {
		w.WriteHeader(400)
		w.Write([]byte("Health Check URL not Configured for origin: " + c.config.Name))
		return
	}

	req, _ := http.NewRequest(c.healthMethod, c.healthURL.String(), nil)
	rsc := request.GetResources(r)
	req = req.WithContext(tctx.WithHealthCheckFlag(tctx.WithResources(context.Background(), rsc), true))

	engines.DoProxy(w, req, true)
}

func (c *Client) populateHeathCheckRequestValues() {
	oc := c.config

	if oc.HealthCheckUpstreamPath == "-" {
		oc.HealthCheckUpstreamPath = "/health"
	}
	if oc.HealthCheckVerb == "-" {
		oc.HealthCheckVerb = http.MethodGet
	}

	c.healthMethod = oc.HealthCheckVerb

	c.healthURL = urls.Clone(c.baseUpstreamURL)
	c.healthURL.Path += oc.HealthCheckUpstreamPath
}
