package redisserver

import (
	"testing"
)

func TestRedisPatternToMqtt(t *testing.T) {
	tests := []struct {
		pattern      string
		wantFilter   string
		hasPost      bool
		matchTopics  []string
		rejectTopics []string
	}{
		{
			pattern:     "*",
			wantFilter:  "#",
			hasPost:     false,
			matchTopics: []string{"a", "a/b", "a/b/c"},
		},
		{
			pattern:     "a/+/c",
			wantFilter:  "a/+/c",
			hasPost:     false,
			matchTopics: []string{"a/x/c"},
		},
		{
			pattern:     "a/#",
			wantFilter:  "a/#",
			hasPost:     false,
			matchTopics: []string{"a/x", "a/x/y"},
		},
		{
			pattern:     `a\*`,
			wantFilter:  "a*",
			hasPost:     false,
			matchTopics: []string{"a*"},
		},
		{
			pattern:      "a/*",
			wantFilter:   "a/#",
			hasPost:      true,
			matchTopics:  []string{"a/b", "a/b/c"},
			rejectTopics: []string{"a"},
		},
		{
			pattern:     "a/*/c",
			wantFilter:  "a/+/c",
			hasPost:     false,
			matchTopics: []string{"a/x/c"},
		},
		{
			pattern:     "*/temp",
			wantFilter:  "+/temp",
			hasPost:     false,
			matchTopics: []string{"sensor/temp"},
		},
		{
			pattern:      "a/b*",
			wantFilter:   "a/#",
			hasPost:      true,
			matchTopics:  []string{"a/b1/x", "a/box", "a/b"},
			rejectTopics: []string{"a/c", "a/x"},
		},
		{
			pattern:      "sensor*",
			wantFilter:   "#",
			hasPost:      true,
			matchTopics:  []string{"sensor1", "sensor/temp"},
			rejectTopics: []string{"device/sensor"},
		},
		{
			pattern:      "a/[xy]/c?",
			wantFilter:   "a/#",
			hasPost:      true,
			matchTopics:  []string{"a/x/c1", "a/y/cz"},
			rejectTopics: []string{"a/z/c1", "a/x/c12"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.pattern, func(t *testing.T) {
			filter, post := redisPatternToMqtt(tc.pattern)
			if filter != tc.wantFilter {
				t.Errorf("filter for %q = %q, want %q", tc.pattern, filter, tc.wantFilter)
			}
			if (post != nil) != tc.hasPost {
				t.Errorf("hasPost for %q = %v, want %v", tc.pattern, post != nil, tc.hasPost)
			}
			if post != nil {
				for _, topic := range tc.matchTopics {
					if !post(topic) {
						t.Errorf("postFilter for %q rejected %q, expected match", tc.pattern, topic)
					}
				}
				for _, topic := range tc.rejectTopics {
					if post(topic) {
						t.Errorf("postFilter for %q matched %q, expected reject", tc.pattern, topic)
					}
				}
			}
		})
	}
}
