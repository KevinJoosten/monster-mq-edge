package prometheus

import (
	"math"
	"testing"
)

func TestJavaDoubleAndParseJavaDouble(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{0, "0.0"},
		{math.Copysign(0, -1), "-0.0"},
		{42, "42.0"},
		{-42, "-42.0"},
		{0.5, "0.5"},
		{21.5, "21.5"},
		{1e7, "1.0E7"},
		{1e6, "1000000.0"},
		{9999999, "9999999.0"},
		{1e-3, "0.001"},
		{1e-4, "1.0E-4"},
		{-1e-4, "-1.0E-4"},
		{1.25e8, "1.25E8"},
		{math.NaN(), "NaN"},
		{math.Inf(1), "Infinity"},
		{math.Inf(-1), "-Infinity"},
	}
	for _, tc := range cases {
		got := javaDouble(tc.in)
		if got != tc.want {
			t.Errorf("javaDouble(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}

	validParse := []struct {
		s    string
		want float64
	}{
		{"42", 42},
		{"  21.5\t\n", 21.5},
		{"1.5d", 1.5},
		{"1.5D", 1.5},
		{"2.5f", 2.5},
		{"2.5F", 2.5},
		{"0x1p3", 8},
		{"0x1.8p1f", 3},
		{"Infinity", math.Inf(1)},
		{"+Infinity", math.Inf(1)},
		{"-Infinity", math.Inf(-1)},
	}
	for _, tc := range validParse {
		got, ok := parseJavaDouble(tc.s)
		if !ok || got != tc.want {
			t.Errorf("parseJavaDouble(%q) = (%v, %v), want (%v, true)", tc.s, got, ok, tc.want)
		}
	}
	if got, ok := parseJavaDouble("NaN"); !ok || !math.IsNaN(got) {
		t.Errorf("parseJavaDouble(\"NaN\") = (%v, %v), want (NaN, true)", got, ok)
	}

	invalidParse := []string{
		"",
		"   ",
		"Inf",
		"+Inf",
		"-Inf",
		"inf",
		"infinity",
		"nan",
		"1_000",
		"0x10",
		"true",
		"abc",
	}
	for _, s := range invalidParse {
		if got, ok := parseJavaDouble(s); ok {
			t.Errorf("parseJavaDouble(%q) = (%v, true), want false", s, got)
		}
	}
}

func TestParsePromQL(t *testing.T) {
	q1 := parsePromQL(`avg_over_time(topics{group="Hist",topic="a/b",field="v"}[5m])`)
	if q1.Function != "AVG" || q1.Window != "5m" || q1.Metric != "topics" || q1.Labels["group"] != "Hist" || q1.Labels["topic"] != "a/b" || q1.Labels["field"] != "v" {
		t.Fatalf("unexpected parsePromQL result: %+v", q1)
	}
	q2 := parsePromQL(`topics{topic="x",topic="y"}`)
	if q2.Function != "" || q2.Metric != "topics" || q2.Labels["topic"] != "y" {
		t.Fatalf("expected last duplicate label to win: %+v", q2)
	}
}
