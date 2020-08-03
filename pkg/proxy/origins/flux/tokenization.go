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
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/tricksterproxy/trickster/pkg/proxy/timeconv"
	"github.com/tricksterproxy/trickster/pkg/timeseries"
	"github.com/tricksterproxy/trickster/pkg/util/regexp/matching"
)

// This file handles tokenization of time parameters within Flux queries
// for cache key hashing and delta proxy caching.

// Tokens for String Interpolation
const (
	tkTime = "<$TIME_TOKEN$>"
)

var reTime, reStep *regexp.Regexp

func init() {
	// Regexp for extracting the step from a Flux Timeseries Query. searches for something like: `aggregateWindow(every: 1d...`
	reStep = regexp.MustCompile(`(?i)aggregateWindow\(\s*every:\s*(?P<step>[0-9]+(ns|µ|u|ms|s|m|h|d|w|y))`)

	reTime = regexp.MustCompile(`(?i)(?P<preOp1>range)\((?P<timeExpr1>start:\s*` +
		`(?P<value1>((?P<ts1>(-|\+|)[0-9]+)(?P<tsUnit1>ns|µ|u|ms|s|m|h|d|w|y)|` +
		`(?P<now1>now\(\))\s*(?P<operand1>(\+|-))\s*` +
		`(?P<offset1>[0-9]+[mhsdwy]))))(\)|` +
		`(?P<timeSep>.*\s*)` +
		`(?P<timeExpr2>stop:\s*` +
		`(?P<value2>((?P<ts2>(-|\+|)[0-9]+)(?P<tsUnit2>ns|µ|u|ms|s|m|h|d|w|y)|` +
		`(?P<now2>now\(\))(\s*(?P<operand2>(\+|-))\s*` +
		`(?P<offset2>[0-9]+[mhsdwy])|))))(.?\)))`)
}

func interpolateTimeQuery(template string, extent *timeseries.Extent) string {
	return strings.Replace(template, tkTime, fmt.Sprintf("start: %s, stop: %s",
		extent.Start.Format(time.RFC3339), extent.End.Format(time.RFC3339)), -1)
}

func getQueryParts(query string) (string, timeseries.Extent) {
	m := matching.GetNamedMatches(reTime, query, nil)
	if _, ok := m["timeExpr2"]; ok {
		done, yes := tokenizeQuery(query, m), parseQueryExtents(query, m)
		return done, yes
	}

	if _, ok := m["timeExpr1"]; !ok {
		// todo: print/return error?
		return "", timeseries.Extent{}
	}

	return tokenizeQuery(query, m), parseQueryExtents(query, m)
}

// tokenizeQuery will take a Flux query and replace all time conditionals with a single $TIME$
func tokenizeQuery(query string, timeParts map[string]string) string {
	replacement := tkTime
	// First check the existence of timeExpr1, and if exists, do the replacement
	// this catches anything with "time >" or "time >="
	if expr, ok := timeParts["timeExpr1"]; ok {
		query = strings.Replace(query, expr, replacement, -1)
		// We already inserted a $TIME$, for any more occurrences, replace with ""
		replacement = ""
	}

	// Then check the existence of timeExpr2, and if exists, do the replacement
	// including any preceding "and" or the following "and" if preceded by "where"
	// this catches anything with "time <" or "time <="
	if expr, ok := timeParts["timeExpr2"]; ok {
		if preOp, ok := timeParts["preOp1"]; ok {
			if strings.ToLower(preOp) == "range" {
				if tSep, ok := timeParts["timeSep"]; ok {
					expr = tSep + expr
				}
			}
		}
		query = strings.Replace(query, expr, replacement, -1)
	}

	return query
}

func parseQueryExtents(query string, timeParts map[string]string) timeseries.Extent {
	var e timeseries.Extent

	t1 := timeFromParts("1", timeParts)
	e.Start = t1

	// todo: verify this returns now if no 2nd part
	t2 := timeFromParts("2", timeParts)
	e.End = t2
	return e
}

func timeFromParts(clauseNum string, timeParts map[string]string) time.Time {
	ts := int64(0)
	nowUnix := time.Now().Unix()

	if _, ok := timeParts["now"+clauseNum]; ok {
		if offset, ok := timeParts["offset"+clauseNum]; ok {
			s, err := timeconv.ParseDuration(offset)
			if err == nil {
				if operand, ok := timeParts["operand"+clauseNum]; ok {
					if operand == "+" {
						ts = nowUnix + int64(s.Seconds())
					} else {
						ts = nowUnix - int64(s.Seconds())
					}
				}
			}
		} else {
			ts = nowUnix
		}
	} else if v, ok := timeParts["value"+clauseNum]; ok {
		s, err := time.ParseDuration(v)
		if err == nil {
			ts = nowUnix + int64(s.Seconds())
		}
	}
	if ts == 0 {
		ts = nowUnix
	}
	return time.Unix(ts, 0)
}
