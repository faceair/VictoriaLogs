package querier

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logql"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// ParseMetricSelector parses s containing PromQL metric selector
// and returns the corresponding LabelFilters.
func ParseMetricSelector(s string) ([]storage.TagFilter, error) {
	expr, err := parsePromQLWithCache(s)
	if err != nil {
		return nil, err
	}
	me, ok := expr.(*logql.MetricExpr)
	if !ok {
		return nil, fmt.Errorf("expecting metricSelector; got %q", expr.AppendString(nil))
	}
	if len(me.LabelFilters) == 0 {
		return nil, fmt.Errorf("labelFilters cannot be empty")
	}
	tfs := toTagFilters(me.LabelFilters)
	return tfs, nil
}
