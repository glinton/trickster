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

// Package routing is the Trickster Request Router
package routing

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"sort"
	"strings"

	"github.com/tricksterproxy/trickster/pkg/cache"
	"github.com/tricksterproxy/trickster/pkg/config"
	"github.com/tricksterproxy/trickster/pkg/proxy/methods"
	"github.com/tricksterproxy/trickster/pkg/proxy/origins"
	"github.com/tricksterproxy/trickster/pkg/proxy/origins/clickhouse"
	"github.com/tricksterproxy/trickster/pkg/proxy/origins/flux"
	"github.com/tricksterproxy/trickster/pkg/proxy/origins/influxdb"
	"github.com/tricksterproxy/trickster/pkg/proxy/origins/irondb"
	oo "github.com/tricksterproxy/trickster/pkg/proxy/origins/options"
	"github.com/tricksterproxy/trickster/pkg/proxy/origins/prometheus"
	"github.com/tricksterproxy/trickster/pkg/proxy/origins/reverseproxycache"
	"github.com/tricksterproxy/trickster/pkg/proxy/origins/rule"
	"github.com/tricksterproxy/trickster/pkg/proxy/origins/types"
	"github.com/tricksterproxy/trickster/pkg/proxy/paths/matching"
	po "github.com/tricksterproxy/trickster/pkg/proxy/paths/options"
	"github.com/tricksterproxy/trickster/pkg/proxy/request/rewriter"
	"github.com/tricksterproxy/trickster/pkg/tracing"
	tl "github.com/tricksterproxy/trickster/pkg/util/log"
	"github.com/tricksterproxy/trickster/pkg/util/middleware"

	"github.com/gorilla/mux"
)

// RegisterPprofRoutes will register the Pprof Debugging endpoints to the provided router
func RegisterPprofRoutes(routerName string, h *http.ServeMux, log *tl.Logger) {
	log.Info("registering pprof /debug routes", tl.Pairs{"routerName": routerName})
	h.HandleFunc("/debug/pprof/", pprof.Index)
	h.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	h.HandleFunc("/debug/pprof/profile", pprof.Profile)
	h.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	h.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

// RegisterProxyRoutes iterates the Trickster Configuration and
// registers the routes for the configured origins
func RegisterProxyRoutes(conf *config.Config, router *mux.Router,
	caches map[string]cache.Cache, tracers tracing.Tracers,
	log *tl.Logger, dryRun bool) (origins.Origins, error) {

	// a fake "top-level" origin representing the main frontend, so rules can route
	// to it via the clients map
	tlo, _ := reverseproxycache.NewClient("frontend", &oo.Options{}, router, nil)

	// proxyClients maintains a list of proxy clients configured for use by Trickster
	var clients = origins.Origins{"frontend": tlo}
	var err error

	defaultOrigin := ""
	var ndo *oo.Options // points to the origin config named "default"
	var cdo *oo.Options // points to the origin config with IsDefault set to true

	// This iteration will ensure default origins are handled properly
	for k, o := range conf.Origins {

		if !types.IsValidOriginType(o.OriginType) {
			return nil,
				fmt.Errorf(`unknown origin type in origin config. originName: %s, originType: %s`,
					k, o.OriginType)
		}

		// Ensure only one default origin exists
		if o.IsDefault {
			if cdo != nil {
				return nil,
					fmt.Errorf("only one origin can be marked as default. Found both %s and %s",
						defaultOrigin, k)
			}
			log.Debug("default origin identified", tl.Pairs{"name": k})
			defaultOrigin = k
			cdo = o
			continue
		}

		// handle origin named "default" last as it needs special
		// handling based on a full pass over the range
		if k == "default" {
			ndo = o
			continue
		}

		_, err = registerOriginRoutes(router, conf, k, o, clients, caches, tracers, log, dryRun)
		if err != nil {
			return nil, err
		}
	}

	if ndo != nil {
		if cdo == nil {
			ndo.IsDefault = true
			cdo = ndo
			defaultOrigin = "default"
		} else {
			_, err = registerOriginRoutes(router, conf, "default", ndo, clients, caches, tracers, log, dryRun)
			if err != nil {
				return nil, err
			}
		}
	}

	if cdo != nil {
		clients, err = registerOriginRoutes(router, conf, defaultOrigin, cdo, clients, caches, tracers, log, dryRun)
		if err != nil {
			return nil, err
		}
	}

	err = validateRuleClients(clients, conf.CompiledRewriters)
	if err != nil {
		return nil, err
	}

	return clients, nil
}

// This ensures that rule clients are fully loaded, which can't be done
// until all origins are processed, so the rule's destination origin names
// can be mapped to their respective clients
func validateRuleClients(clients origins.Origins,
	rwi map[string]rewriter.RewriteInstructions) error {

	ruleClients := make(rule.Clients, 0, len(clients))
	for _, c := range clients {
		if rc, ok := c.(*rule.Client); ok {
			ruleClients = append(ruleClients, rc)
		}
	}
	if len(ruleClients) > 0 {
		if err := ruleClients.Validate(rwi); err != nil {
			return err
		}
	}
	return nil
}

func registerOriginRoutes(router *mux.Router, conf *config.Config, k string,
	o *oo.Options, clients origins.Origins, caches map[string]cache.Cache,
	tracers tracing.Tracers, log *tl.Logger, dryRun bool) (origins.Origins, error) {

	var client origins.Client
	var c cache.Cache
	var ok bool
	var err error

	c, ok = caches[o.CacheName]
	if !ok {
		return nil, fmt.Errorf("could not find cache named [%s]", o.CacheName)
	}

	if !dryRun {
		log.Info("registering route paths", tl.Pairs{"originName": k,
			"originType": o.OriginType, "upstreamHost": o.Host})
	}

	switch strings.ToLower(o.OriginType) {
	case "prometheus", "":
		client, err = prometheus.NewClient(k, o, mux.NewRouter(), c)
	case "influxdb":
		client, err = influxdb.NewClient(k, o, mux.NewRouter(), c)
	case "flux":
		client, err = flux.NewClient(k, o, mux.NewRouter(), c)
	case "irondb":
		client, err = irondb.NewClient(k, o, mux.NewRouter(), c)
	case "clickhouse":
		client, err = clickhouse.NewClient(k, o, mux.NewRouter(), c)
	case "rpc", "reverseproxycache":
		client, err = reverseproxycache.NewClient(k, o, mux.NewRouter(), c)
	case "rule":
		client, err = rule.NewClient(k, o, mux.NewRouter(), clients)
	}
	if err != nil {
		return nil, err
	}

	if client != nil && !dryRun {
		o.HTTPClient = client.HTTPClient()
		clients[k] = client
		defaultPaths := client.DefaultPathConfigs(o)
		registerPathRoutes(router, client.Handlers(), client, o, c, defaultPaths,
			tracers, conf.Main.HealthHandlerPath, log)
	}
	return clients, nil
}

// registerPathRoutes will take the provided default paths map,
// merge it with any path data in the provided originconfig, and then register
// the path routes to the appropriate handler from the provided handlers map
func registerPathRoutes(router *mux.Router, handlers map[string]http.Handler,
	client origins.Client, oo *oo.Options, c cache.Cache,
	defaultPaths map[string]*po.Options, tracers tracing.Tracers,
	healthHandlerPath string, log *tl.Logger) {

	if oo == nil {
		return
	}

	// get the distributed tracer if configured
	var tr *tracing.Tracer
	if oo != nil {
		if t, ok := tracers[oo.TracingConfigName]; ok {
			tr = t
		}
	}

	decorate := func(po *po.Options) http.Handler {
		// default base route is the path handler
		h := po.Handler
		// attach distributed tracer
		if tr != nil {
			h = middleware.Trace(tr, h)
		}
		// add Origin, Cache, and Path Configs to the HTTP Request's context
		h = middleware.WithResourcesContext(client, oo, c, po, tr, log, h)
		// attach any request rewriters
		if len(oo.ReqRewriter) > 0 {
			h = rewriter.Rewrite(oo.ReqRewriter, h)
		}
		if len(po.ReqRewriter) > 0 {
			h = rewriter.Rewrite(po.ReqRewriter, h)
		}
		// decorate frontend prometheus metrics
		if !po.NoMetrics {
			h = middleware.Decorate(oo.Name, oo.OriginType, po.Path, h)
		}
		return h
	}

	// now we'll go ahead and register the health handler
	if h, ok := handlers["health"]; ok &&
		oo.HealthCheckUpstreamPath != "" && oo.HealthCheckVerb != "" && healthHandlerPath != "" {
		hp := strings.Replace(healthHandlerPath+"/"+oo.Name, "//", "/", -1)
		log.Debug("registering health handler path",
			tl.Pairs{"path": hp, "originName": oo.Name,
				"upstreamPath": oo.HealthCheckUpstreamPath,
				"upstreamVerb": oo.HealthCheckVerb})
		router.PathPrefix(hp).
			Handler(middleware.WithResourcesContext(client, oo, nil, nil, tr, log, h)).
			Methods(methods.CacheableHTTPMethods()...)
	}

	// This takes the default paths, named like '/api/v1/query' and morphs the name
	// into what the router wants, with methods like '/api/v1/query-GET-HEAD', to help
	// route sorting
	pathsWithVerbs := make(map[string]*po.Options)
	for _, p := range defaultPaths {
		if len(p.Methods) == 0 {
			p.Methods = methods.CacheableHTTPMethods()
		}
		pathsWithVerbs[p.Path+"-"+strings.Join(p.Methods, "-")] = p
	}

	// now we will iterate through the configured paths, and overlay them on those default paths.
	// for a rule origin type, only the default paths are used with no overlay or importable config
	if oo.OriginType != "rule" {
		for k, p := range oo.Paths {
			if p2, ok := pathsWithVerbs[k]; ok {
				p2.Merge(p)
				continue
			}
			p3 := po.NewOptions()
			p3.Merge(p)
			pathsWithVerbs[k] = p3
		}
	}

	plist := make([]string, 0, len(pathsWithVerbs))
	deletes := make([]string, 0, len(pathsWithVerbs))
	for k, p := range pathsWithVerbs {
		if h, ok := handlers[p.HandlerName]; ok && h != nil {
			p.Handler = h
			plist = append(plist, k)
		} else {
			log.Info("invalid handler name for path",
				tl.Pairs{"path": p.Path, "handlerName": p.HandlerName})
			deletes = append(deletes, p.Path)
		}
	}
	for _, p := range deletes {
		delete(pathsWithVerbs, p)
	}

	sort.Sort(ByLen(plist))
	for i := len(plist)/2 - 1; i >= 0; i-- {
		opp := len(plist) - 1 - i
		plist[i], plist[opp] = plist[opp], plist[i]
	}

	or := client.Router().(*mux.Router)

	for _, v := range plist {
		p := pathsWithVerbs[v]

		pathPrefix := "/" + oo.Name
		handledPath := pathPrefix + p.Path

		log.Debug("registering origin handler path",
			tl.Pairs{"originName": oo.Name, "path": v, "handlerName": p.HandlerName,
				"originHost": oo.Host, "handledPath": handledPath, "matchType": p.MatchType,
				"frontendHosts": strings.Join(oo.Hosts, ",")})
		if p.Handler != nil && len(p.Methods) > 0 {

			if p.Methods[0] == "*" {
				p.Methods = methods.AllHTTPMethods()
			}

			switch p.MatchType {
			case matching.PathMatchTypePrefix:
				// Case where we path match by prefix
				// Host Header Routing
				for _, h := range oo.Hosts {
					router.PathPrefix(p.Path).Handler(decorate(p)).Methods(p.Methods...).Host(h)
				}
				if !oo.PathRoutingDisabled {
					// Path Routing
					router.PathPrefix(handledPath).Handler(middleware.StripPathPrefix(pathPrefix, decorate(p))).Methods(p.Methods...)
				}
				or.PathPrefix(p.Path).Handler(decorate(p)).Methods(p.Methods...)
			default:
				// default to exact match
				// Host Header Routing
				for _, h := range oo.Hosts {
					router.Handle(p.Path, decorate(p)).Methods(p.Methods...).Host(h)
				}
				if !oo.PathRoutingDisabled {
					// Path Routing
					router.Handle(handledPath, middleware.StripPathPrefix(pathPrefix, decorate(p))).Methods(p.Methods...)
				}
				or.Handle(p.Path, decorate(p)).Methods(p.Methods...)
			}
		}
	}

	if oo.IsDefault {
		log.Info("registering default origin handler paths", tl.Pairs{"originName": oo.Name})
		for _, v := range plist {
			p := pathsWithVerbs[v]
			if p.Handler != nil && len(p.Methods) > 0 {
				log.Debug("registering default origin handler paths",
					tl.Pairs{"originName": oo.Name, "path": p.Path, "handlerName": p.HandlerName,
						"matchType": p.MatchType})
				switch p.MatchType {
				case matching.PathMatchTypePrefix:
					// Case where we path match by prefix
					router.PathPrefix(p.Path).Handler(decorate(p)).Methods(p.Methods...)
				default:
					// default to exact match
					router.Handle(p.Path, decorate(p)).Methods(p.Methods...)
				}
				router.Handle(p.Path, decorate(p)).Methods(p.Methods...)
			}
		}
	}
	oo.Router = or
	oo.Paths = pathsWithVerbs
}

// ByLen allows sorting of a string slice by string length
type ByLen []string

func (a ByLen) Len() int {
	return len(a)
}

func (a ByLen) Less(i, j int) bool {
	return len(a[i]) < len(a[j])
}

func (a ByLen) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
