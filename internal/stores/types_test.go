package stores

import "testing"

func TestAggregatedColumnName(t *testing.T) {
	tests := []struct {
		topic string
		field string
		fn    string
		want  string
	}{
		{"a/b", "x.y", "AVG", "a/b.x_y_avg"},
		{"a/b", "", "max", "a/b_max"},
		{"a/b/c", "data.sub.val", "Min", "a/b/c.data_sub_val_min"},
		{"sensor/temp", "", "COUNT", "sensor/temp_count"},
	}

	for _, tt := range tests {
		got := AggregatedColumnName(tt.topic, tt.field, tt.fn)
		if got != tt.want {
			t.Errorf("AggregatedColumnName(%q, %q, %q) = %q, want %q", tt.topic, tt.field, tt.fn, got, tt.want)
		}
	}
}
