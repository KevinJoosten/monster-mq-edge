package prometheus

import (
	"testing"

	"monstermq.io/edge/internal/config"
)

func TestListenAddr(t *testing.T) {
	cases := []struct {
		name string
		cfg  *config.Config
		want string
	}{
		{"nil config", nil, "0.0.0.0:3001"},
		{"address unset", &config.Config{Prometheus: config.PrometheusConfig{Port: 9100}}, "0.0.0.0:9100"},
		{"loopback", &config.Config{Prometheus: config.PrometheusConfig{Port: 9100, Address: "127.0.0.1"}}, "127.0.0.1:9100"},
		{"ipv6 loopback", &config.Config{Prometheus: config.PrometheusConfig{Port: 9100, Address: "::1"}}, "[::1]:9100"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New(Deps{Cfg: tc.cfg})
			if got := s.listenAddr(); got != tc.want {
				t.Fatalf("listenAddr() = %q, want %q", got, tc.want)
			}
		})
	}
}
