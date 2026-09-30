package version

import "testing"

// TestString illustre le style "table-driven" idiomatique en Go :
// une table de cas, une boucle, un sous-test nommé par cas.
func TestString(t *testing.T) {
	original := Version
	t.Cleanup(func() { Version = original })

	cases := []struct {
		name    string
		version string
		want    string
	}{
		{name: "valeur par défaut", version: "dev", want: "pulse-stream dev"},
		{name: "version taguée", version: "v0.1.0", want: "pulse-stream v0.1.0"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			Version = tc.version
			if got := String(); got != tc.want {
				t.Errorf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}
