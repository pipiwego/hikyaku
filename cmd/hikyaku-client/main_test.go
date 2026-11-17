package main

import (
	"reflect"
	"testing"
)

func TestParseServers(t *testing.T) {
	cases := []struct {
		name   string
		values []string
		want   []string
		err    bool
	}{
		{"none", nil, nil, true},
		{"empty and commas", []string{"", " , ,", "  "}, nil, true},
		{"trim and order", []string{" [2001:db8::a]:8444 , [2001:db8::b]:8444 ", " [2001:db8::a]:8444 "}, []string{"[2001:db8::a]:8444", "[2001:db8::b]:8444"}, false},
		{"repeat and comma", []string{"[a]:1", "[b]:2,[c]:3", "[a]:1,, [d]:4"}, []string{"[a]:1", "[b]:2", "[c]:3", "[d]:4"}, false},
	}
	for _, c := range cases {
		got, err := parseServers(c.values)
		if c.err {
			if err == nil {
				t.Errorf("%s: no error, want one", c.name)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}
