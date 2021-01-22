package storage

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

func (is *indexSearch) getMetricIDsByTimeRangeAndFilters(minTimestamp, maxTimestamp uint64, tfs *TagFilters, limit int) (*uint64set.Set, error) {
	// Sort tfs by the number of matching filters from previous queries.
	// This way we limit the amount of work below by applying more specific filters at first.
	type tagFilterWithCount struct {
		tf    *tagFilter
		cost  uint64
		count uint64
	}
	date := minTimestamp / msecPerDay
	tfsWithCount := make([]tagFilterWithCount, len(tfs.tfs))
	kb := &is.kb
	var buf []byte
	for i := range tfs.tfs {
		tf := &tfs.tfs[i]
		kb.B = appendDateTagFilterCacheKey(kb.B[:0], date, tf, is.accountID, is.projectID)
		buf = is.db.metricIDsPerDateTagFilterCache.Get(buf[:0], kb.B)
		count := uint64(0)
		if len(buf) == 8 {
			count = encoding.UnmarshalUint64(buf)
		}
		tfsWithCount[i] = tagFilterWithCount{
			tf:    tf,
			cost:  count * tf.matchCost,
			count: count,
		}
	}
	sort.Slice(tfsWithCount, func(i, j int) bool {
		a, b := &tfsWithCount[i], &tfsWithCount[j]
		if a.cost != b.cost {
			return a.cost < b.cost
		}
		return a.tf.Less(b.tf)
	})

	// Populate metricIDs with the first non-negative filter.
	var metricIDs *uint64set.Set
	maxLimit := limit * 100
	tfsRemainingWithCount := tfsWithCount[:0]
	for i := range tfsWithCount {
		tf := tfsWithCount[i].tf
		if tf.isNegative {
			tfsRemainingWithCount = append(tfsRemainingWithCount, tfsWithCount[i])
			continue
		}
		m, err := is.getMetricIDsByTimeRangeTagFilter(tf, minTimestamp, maxTimestamp, tfs.commonPrefix, nil, maxLimit)
		if err != nil {
			return nil, err
		}
		metricIDs = m
		i++
		for i < len(tfsWithCount) {
			tfsRemainingWithCount = append(tfsRemainingWithCount, tfsWithCount[i])
			i++
		}
		break
	}
	if metricIDs.Len() == 0 {
		// There is no sense in inspecting tfsRemainingWithCount, since the result will be empty.
		return nil, nil
	}

	// Intersect metricIDs with the rest of filters.
	for i := range tfsRemainingWithCount {
		tfWithCount := tfsRemainingWithCount[i]
		tf := tfWithCount.tf
		m, err := is.getMetricIDsByTimeRangeTagFilter(tf, minTimestamp, maxTimestamp, tfs.commonPrefix, metricIDs, maxLimit)
		if err != nil {
			return nil, err
		}
		if tf.isNegative {
			metricIDs.Subtract(m)
		} else {
			metricIDs.Intersect(m)
		}
		if metricIDs.Len() == 0 {
			// Short circuit - there is no need in applying the remaining filters to empty set.
			return nil, nil
		}
	}
	return metricIDs, nil
}

func (is *indexSearch) getMetricIDsByTimeRangeTagFilter(tf *tagFilter, minTimestamp, maxTimestamp uint64, commonPrefix []byte, filter *uint64set.Set, limit int) (*uint64set.Set, error) {
	// Augument tag filter prefix for per-minute search instead of global search.
	if !bytes.HasPrefix(tf.prefix, commonPrefix) {
		logger.Panicf("BUG: unexpected tf.prefix %q; must start with commonPrefix %q", tf.prefix, commonPrefix)
	}
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagTimeToMetricIDs)
	kb.B = append(kb.B, tf.prefix[len(commonPrefix):]...)

	tfNew := *tf
	tfNew.isNegative = false // isNegative for the original tf is handled by the caller.
	tfNew.prefix = kb.B
	tfNew.rangeMinTime = minTimestamp
	tfNew.rangeMaxTime = maxTimestamp
	metricIDs, err := is.getMetricIDsByTimeRangeForTagFilter(&tfNew, filter, limit)
	if filter != nil {
		// Do not cache the number of matching metricIDs,
		// since this number may be modified by filter.
		return metricIDs, err
	}

	date := minTimestamp / msecPerDay
	// Store the number of matching metricIDs in the cache in order to sort tag filters
	// in ascending number of matching metricIDs on the next search.
	is.kb.B = appendDateTagFilterCacheKey(is.kb.B[:0], date, tf, is.accountID, is.projectID)
	metricIDsLen := uint64(metricIDs.Len())
	if err != nil {
		// Set metricIDsLen to maxMetrics, so the given entry will be moved to the end
		// of tag filters on the next search.
		metricIDsLen = uint64(limit)
	}
	kb.B = encoding.MarshalUint64(kb.B[:0], metricIDsLen)
	is.db.metricIDsPerDateTagFilterCache.Set(is.kb.B, kb.B)
	return metricIDs, err
}

func (is *indexSearch) getMetricIDsByTimeRangeForTagFilter(tf *tagFilter, filter *uint64set.Set, limit int) (*uint64set.Set, error) {
	if tf.isNegative {
		logger.Panicf("BUG: isNegative must be false")
	}
	metricIDs := &uint64set.Set{}
	if len(tf.orSuffixes) != 1 && tf.orSuffixes[0] != "" {
		return nil, fmt.Errorf("can't find metricid by range with %s", tf)
	}

	if err := is.updateMetricIDsByTimeRangeForOrSuffixesNoFilter(tf, limit, metricIDs); err != nil {
		if err == errFallbackToMetricNameMatch {
			return nil, err
		}
		return nil, fmt.Errorf("error when searching for metricIDs for tagFilter in fast path: %w; tagFilter=%s", err, tf)
	}
	return metricIDs, nil
}

func (is *indexSearch) updateMetricIDsByTimeRangeForOrSuffixesNoFilter(tf *tagFilter, limit int, metricIDs *uint64set.Set) error {
	if tf.isNegative {
		logger.Panicf("BUG: isNegative must be false")
	}
	startKB := kbPool.Get()
	defer kbPool.Put(startKB)

	endKB := kbPool.Get()
	defer kbPool.Put(endKB)

	for _, orSuffix := range tf.orSuffixes {
		startKB.B = append(startKB.B[:0], tf.prefix...)
		startKB.B = append(startKB.B, orSuffix...)
		startKB.B = append(startKB.B, tagSeparatorChar)
		startKB.B = encoding.MarshalUint64(startKB.B, tf.rangeMinTime)

		endKB.B = append(endKB.B[:0], tf.prefix...)
		endKB.B = append(endKB.B, orSuffix...)
		endKB.B = append(endKB.B, tagSeparatorChar)
		endKB.B = encoding.MarshalUint64(endKB.B, tf.rangeMaxTime)

		if err := is.updateMetricIDsByTimeRangeForOrSuffixNoFilter(startKB.B, endKB.B, limit, metricIDs); err != nil {
			return err
		}
		if metricIDs.Len() >= limit {
			return nil
		}
	}
	return nil
}

func (is *indexSearch) updateMetricIDsByTimeRangeForOrSuffixNoFilter(startPrefix, endPrefix []byte, limit int, metricIDs *uint64set.Set) error {
	ts := &is.ts
	mp := &is.mp
	mp.Reset()
	loopsPaceLimiter := 0
	ts.Seek(startPrefix)
	for metricIDs.Len() < limit && ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if bytes.Compare(item, endPrefix) == 1 {
			return nil
		}
		if err := mp.InitOnlyTail(item, item[len(startPrefix):]); err != nil {
			return err
		}
		mp.ParseMetricIDs()
		remain := limit - metricIDs.Len()
		if len(mp.MetricIDs) > remain {
			metricIDs.AddMulti(mp.MetricIDs[:remain])
			return nil
		}
		metricIDs.AddMulti(mp.MetricIDs)
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching for tag filter prefix %q-%q: %w", startPrefix, endPrefix, err)
	}
	return nil
}
