package collector

import "testing"

func TestAnonymizeIP(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"IPv4 : dernier octet à zéro", "203.0.113.42", "203.0.113.0"},
		{"IPv4 déjà tronquée", "203.0.113.0", "203.0.113.0"},
		{"IPv6 : on garde 48 bits", "2001:db8:85a3:8d3:1319:8a2e:370:7348", "2001:db8:85a3::"},
		{"IPv6 avec zone", "fe80::1%eth0", "fe80::"},
		{"IPv4 transportée en IPv6", "::ffff:203.0.113.42", "203.0.113.0"},
		{"boucle locale IPv6", "::1", "::"},
		{"valeur illisible", "not-an-ip", ""},
		{"vide", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := anonymizeIP(c.in); got != c.want {
				t.Errorf("anonymizeIP(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}
