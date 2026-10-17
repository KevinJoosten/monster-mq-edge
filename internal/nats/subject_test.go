package nats

import "testing"

func TestSubjectConversion(t *testing.T) {
	cases := []struct {
		subject string
		topic   string
	}{
		{"sensors.temp.room1", "sensors/temp/room1"},
		{"sensors.*.room1", "sensors/+/room1"},
		{"sensors.>", "sensors/#"},
	}
	for _, tc := range cases {
		if got := SubjectToTopic(tc.subject); got != tc.topic {
			t.Errorf("SubjectToTopic(%q) = %q; want %q", tc.subject, got, tc.topic)
		}
	}

	topicCases := []struct {
		topic   string
		subject string
	}{
		{"sensors/temp/room1", "sensors.temp.room1"},
		{"sensors/+/room1", "sensors.*.room1"},
		{"sensors/#", "sensors.>"},
		{"room 1/t", "room_1.t"},
	}
	for _, tc := range topicCases {
		if got := TopicToSubject(tc.topic); got != tc.subject {
			t.Errorf("TopicToSubject(%q) = %q; want %q", tc.topic, got, tc.subject)
		}
	}
}

func TestTopicMatchesFilter(t *testing.T) {
	if !topicMatchesFilter("a", "a/#") {
		t.Errorf("expected 'a' to match 'a/#'")
	}
	if !topicMatchesFilter("a/b", "a/#") {
		t.Errorf("expected 'a/b' to match 'a/#'")
	}
	if topicMatchesFilter("b", "a/#") {
		t.Errorf("expected 'b' not to match 'a/#'")
	}
	if !topicMatchesFilter("a/b/c", "#") {
		t.Errorf("expected 'a/b/c' to match '#'")
	}
	if !topicMatchesFilter("a/b/c", "a/+/c") {
		t.Errorf("expected 'a/b/c' to match 'a/+/c'")
	}
	if topicMatchesFilter("a/b/c/d", "a/+/c") {
		t.Errorf("expected 'a/b/c/d' not to match 'a/+/c'")
	}
}
