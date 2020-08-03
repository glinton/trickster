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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tricksterproxy/trickster/pkg/sort/times"
	"github.com/tricksterproxy/trickster/pkg/timeseries"
)

// SetExtents overwrites a Timeseries's known extents with the provided extent list
func (se *SeriesEnvelope) SetExtents(extents timeseries.ExtentList) {
	el := make(timeseries.ExtentList, len(extents))
	copy(el, extents)
	se.ExtentList = el
	se.isCounted = false
}

// Extents returns the Timeseries's ExentList
func (se *SeriesEnvelope) Extents() timeseries.ExtentList {
	return se.ExtentList
}

// ValueCount returns the count of all values across all series in the Timeseries
func (se *SeriesEnvelope) ValueCount() int {
	// assume one field per record for now
	return len(se.Results.Records)
}

// TimestampCount returns the count of unique timestampes across all series in the Timeseries
func (se *SeriesEnvelope) TimestampCount() int {
	return len(se.Results.Records)
	// if se.timestamps == nil {
	// 	se.timestamps = make(map[time.Time]bool)
	// }
	// se.updateTimestamps()
	// return len(se.timestamps)
}

// func (se *SeriesEnvelope) updateTimestamps() {
// 	if se.isCounted {
// 		return
// 	}
// 	m := make(map[time.Time]bool)

// 	for i := range se.Results {
// 		for j, s := range se.Results[i].Series {
// 			ti := str.IndexOfString(s.Columns, "time")
// 			if ti < 0 {
// 				continue
// 			}
// 			for k := range se.Results[i].Series[j].Values {
// 				m[time.Unix(int64(se.Results[i].Series[j].Values[k][ti].(float64)/1000), 0)] = true
// 			}
// 		}
// 	}

// 	se.timestamps = m
// 	se.tslist = times.FromMap(m)
// 	se.isCounted = true
// }

// SeriesCount returns the count of all Results in the Timeseries
// it is called SeriesCount due to Interface conformity and the disparity in nomenclature between various TSDBs.
func (se *SeriesEnvelope) SeriesCount() int {
	return len(se.Results.Records)
}

// Step returns the step for the Timeseries
func (se *SeriesEnvelope) Step() time.Duration {
	return se.StepDuration
}

// SetStep sets the step for the Timeseries
func (se *SeriesEnvelope) SetStep(step time.Duration) {
	se.StepDuration = step
}

type seriesKey struct {
	ResultID    int
	StatementID int
	Name        string
	Tags        string
	Columns     string
}

type tags map[string]string

func (t tags) String() string {
	if len(t) == 0 {
		return ""
	}
	pairs := make(sort.StringSlice, len(t))
	var i int
	for k, v := range t {
		pairs[i] = fmt.Sprintf("%s=%s", k, v)
		i++
	}
	sort.Sort(pairs)
	return strings.Join(pairs, ";")
}

// Merge merges the provided Timeseries list into the base Timeseries
// (in the order provided) and optionally sorts the merged Timeseries
func (se *SeriesEnvelope) Merge(sort bool, collection ...timeseries.Timeseries) {
	mtx := sync.Mutex{}
	wg := sync.WaitGroup{}

	se.updateLock.Lock()
	defer se.updateLock.Unlock()

	series := make(map[string][]string)
	for i := range se.Results.Records {
		wg.Add(1)
		go func(s []string) {
			mtx.Lock()
			series[s[se.tsIndex]] = s
			mtx.Unlock()
			wg.Done()
		}(se.Results.Records[i])
	}
	wg.Wait()

	for _, ts := range collection {
		if ts != nil {
			se2, ok := ts.(*SeriesEnvelope)
			if !ok {
				continue
			}

			for g := range se2.Results.Records {
				// update cache with new results
				if g >= len(se.Results.Records) {
					mtx.Lock()
					// todo: verify no overlap (shouldn't be due to query)
					se.Results.Records = append(se.Results.Records, se2.Results.Records[g:]...)
					mtx.Unlock()
					break
				}

				wg.Add(1)
				go func(s []string) {
					mtx.Lock()
					series[s[se2.tsIndex]] = s
					mtx.Unlock()
					wg.Done()
				}(se2.Results.Records[g])
			}
			wg.Wait()

			mtx.Lock()
			se.ExtentList = append(se.ExtentList, se2.ExtentList...)
			mtx.Unlock()
		}
	}

	uniqRecords := make([][]string, len(series))
	j := 0
	for i := range series {
		uniqRecords[j] = series[i]
		j++
	}

	(*se).Results.Records = uniqRecords
	se.ExtentList = se.ExtentList.Compress(se.StepDuration)
	se.isSorted = false
	se.isCounted = false
	if sort {
		se.Sort()
	}
}

// Clone returns a perfect copy of the base Timeseries
func (se *SeriesEnvelope) Clone() timeseries.Timeseries {
	se.updateLock.Lock()
	defer se.updateLock.Unlock()
	clone := &SeriesEnvelope{
		Err:          se.Err,
		Results:      se.Results,
		StepDuration: se.StepDuration,
		ExtentList:   make(timeseries.ExtentList, len(se.ExtentList)),
		timestamps:   make(map[time.Time]bool),
		tslist:       make(times.Times, len(se.tslist)),
		isCounted:    se.isCounted,
		isSorted:     se.isSorted,
		tsIndex:      se.tsIndex,
	}

	copy(clone.ExtentList, se.ExtentList)
	for k, v := range se.timestamps {
		clone.timestamps[k] = v
	}

	copy(clone.tslist, se.tslist)

	return clone
}

// CropToSize reduces the number of elements in the Timeseries to the provided count, by evicting elements
// using a least-recently-used methodology. The time parameter limits the upper extent to the provided time,
// in order to support backfill tolerance
func (se *SeriesEnvelope) CropToSize(sz int, t time.Time, lur timeseries.Extent) {
	tc := se.TimestampCount()
	if len(se.Results.Records) == 0 || tc <= sz {
		return
	}

	se.Results.Records = se.Results.Records[sz:]
	se.Results.Records = se.Results.Records[:len(se.Results.Records)-sz]

	// todo: crop other things as well (extentlist, etc...)
	/*
		se.isCounted = false
		se.isSorted = false
		x := len(se.ExtentList)

		// The Series has no extents, so no need to do anything
		if x < 1 {
			// todo: maybe not do this
			se.Results = FluxCSV{}
			se.ExtentList = timeseries.ExtentList{}
			return
		}

		// Crop to the Backfill Tolerance Value if needed
		if se.ExtentList[x-1].End.After(t) {
			se.CropToRange(timeseries.Extent{Start: se.ExtentList[0].Start, End: t})
		}

		tc := se.TimestampCount()
		if len(se.Results.Records) == 0 || tc <= sz {
			return
		}

		el := timeseries.ExtentListLRU(se.ExtentList).UpdateLastUsed(lur, se.StepDuration)
		sort.Sort(el)

		rc := tc - sz // # of required timestamps we must delete to meet the rentention policy
		removals := make(map[time.Time]bool)
		done := false
		var ok bool

		for _, x := range el {
			for ts := x.Start; !x.End.Before(ts) && !done; ts = ts.Add(se.StepDuration) {
				if _, ok = se.timestamps[ts]; ok {
					removals[ts] = true
					done = len(removals) >= rc
				}
			}
			if done {
				break
			}
		}

		for i, r := range se.Results {
			for j, s := range r.Series {
				tmp := se.Results[i].Series[j].Values[:0]
				for _, v := range se.Results[i].Series[j].Values {
					t, err := time.Parse(time.RFC3339, v[se.tsIndex])
					if _, ok := removals[t]; !ok {
						tmp = append(tmp, v)
					}
				}
				s.Values = tmp
			}
		}

		tl := times.FromMap(removals)
		sort.Sort(tl)
		for _, t := range tl {
			for i, e := range el {
				if e.StartsAt(t) {
					el[i].Start = e.Start.Add(se.StepDuration)
				}
			}
		}

		se.ExtentList = timeseries.ExtentList(el).Compress(se.StepDuration)
		se.Sort()
	*/
}

// CropToRange reduces the Timeseries down to timestamps contained within the provided Extents (inclusive).
// CropToRange assumes the base Timeseries is already sorted, and will corrupt an unsorted Timeseries
func (se *SeriesEnvelope) CropToRange(e timeseries.Extent) {
cropRange:
	for i := range se.Results.Records {
		if t, err := time.Parse(time.RFC3339, se.Results.Records[i][se.tsIndex]); err != nil ||
			!(t.After(e.Start) && t.Before(e.End)) {
			se.Results.Records = append(se.Results.Records[:i], se.Results.Records[i+1:]...)
			goto cropRange
			// return
		}
	}

	// todo: crop others too
	/*
		se.isCounted = false
		x := len(se.ExtentList)
		// The Series has no extents, so no need to do anything
		if x < 1 {
			for i := range se.Results {
				se.Results[i].Series = []models.Row{}
			}
			se.ExtentList = timeseries.ExtentList{}
			return
		}

		// if the extent of the series is entirely outside the extent of the crop range, return empty set and bail
		if se.ExtentList.OutsideOf(e) {
			for i := range se.Results {
				se.Results[i].Series = []models.Row{}
			}
			se.ExtentList = timeseries.ExtentList{}
			return
		}

		// if the series extent is entirely inside the extent of the crop range, simple adjust down its ExtentList
		if se.ExtentList.InsideOf(e) {
			if se.ValueCount() == 0 {
				for i := range se.Results {
					se.Results[i].Series = []models.Row{}
				}
			}
			se.ExtentList = se.ExtentList.Crop(e)
			return
		}

		startSecs := e.Start.Unix()
		endSecs := e.End.Unix()

		for i, r := range se.Results {

			if len(r.Series) == 0 {
				se.ExtentList = se.ExtentList.Crop(e)
				continue
			}

			deletes := make(map[int]bool)

			for j, s := range r.Series {
				// check the index of the time column again just in case it changed in the next series
				ti := str.IndexOfString(s.Columns, "time")
				if ti != -1 {
					start := -1
					end := -1
					for vi, v := range se.Results[i].Series[j].Values {
						if v == nil || len(v) <= ti {
							continue
						}

						t := int64(v[ti].(float64) / 1000)
						if t == endSecs {
							if vi == 0 || t == startSecs || start == -1 {
								start = vi
							}
							end = vi + 1
							break
						}
						if t > endSecs {
							end = vi
							break
						}
						if t < startSecs {
							continue
						}
						if start == -1 && (t == startSecs || (endSecs > t && t > startSecs)) {
							start = vi
						}
					}
					if start != -1 {
						if end == -1 {
							end = len(s.Values)
						}
						se.Results[i].Series[j].Values = s.Values[start:end]
					} else {
						deletes[j] = true
					}
				}
			}
			if len(deletes) > 0 {
				tmp := se.Results[i].Series[:0]
				for i, r := range se.Results[i].Series {
					if _, ok := deletes[i]; !ok {
						tmp = append(tmp, r)
					}
				}
				se.Results[i].Series = tmp
			}
		}
		se.ExtentList = se.ExtentList.Crop(e)
	*/
}

func (se *SeriesEnvelope) Len() int {
	return len(se.Results.Records)
}

func (se *SeriesEnvelope) Less(i, j int) bool {
	return se.Results.Records[i][se.tsIndex] < se.Results.Records[j][se.tsIndex]
}

func (se *SeriesEnvelope) Swap(i, j int) {
	se.Results.Records[i], se.Results.Records[j] = se.Results.Records[j], se.Results.Records[i]
}

// Sort sorts all Values in each Series chronologically by their timestamp
func (se *SeriesEnvelope) Sort() {
	if se.isSorted || len(se.Results.Records) == 0 {
		return
	}

	sort.Sort(se)

	// todo: is all this necessary?
	/*
		wg := sync.WaitGroup{}
		mtx := sync.Mutex{}

		var hasWarned bool
		tsm := map[time.Time]bool{}
		if ti := str.IndexOfString(se.Results[0].Series[0].Columns, "time"); ti != -1 {
			for ri := range se.Results {
				for si := range se.Results[ri].Series {
					m := make(map[int64][]interface{})
					keys := make([]int64, 0, len(m))
					for _, v := range se.Results[ri].Series[si].Values {
						wg.Add(1)
						go func(s []interface{}) {
							if s == nil || len(s) <= ti {
								wg.Done()
								return
							}
							if tf, ok := s[ti].(float64); ok {
								t := int64(tf)
								mtx.Lock()
								if _, ok := m[t]; !ok {
									keys = append(keys, t)
									m[t] = s
								}
								tsm[time.Unix(t/1000, 0)] = true
								mtx.Unlock()
							} else if !hasWarned {
								hasWarned = true
								// this makeshift warning is temporary during the beta cycle to help
								// troubleshoot #433
									s[ti], "resultSet:", se)
							}
							wg.Done()
						}(v)
					}
					wg.Wait()
					sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
					sm := make([][]interface{}, 0, len(keys))
					for _, key := range keys {
						sm = append(sm, m[key])
					}
					se.Results[ri].Series[si].Values = sm
				}
			}
		}

		sort.Sort(se.ExtentList)

		se.timestamps = tsm
		se.tslist = times.FromMap(tsm)
		se.isCounted = true
		se.isSorted = true
	*/
}

// Size returns the approximate memory utilization in bytes of the timeseries
func (se *SeriesEnvelope) Size() int {
	c := uint64(24 + // .stepDuration
		len(se.Err) +
		se.ExtentList.Size() + // time.Time (24) * 3
		(25 * len(se.timestamps)) + // time.Time (24) + bool(1)
		(24 * len(se.tslist)) + // time.Time (24)
		2, // .isSorted + .isCounted
	)

	wg := sync.WaitGroup{}
	for i, res := range se.Results.Records {
		for j := range res {
			wg.Add(1)
			go func(r string) {
				// todo: ensure this is correct
				atomic.AddUint64(&c, uint64(len(r)))
				wg.Done()
			}(se.Results.Records[i][j])
		}
	}
	wg.Wait()
	return int(c)
}
