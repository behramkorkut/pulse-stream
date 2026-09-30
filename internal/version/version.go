// Package version expose la version de l'application, injectée à la compilation.
package version

// Version vaut "dev" par défaut. Le Makefile la remplace à la compilation avec :
//
//	go build -ldflags "-X github.com/behramkorkut/pulse-stream/internal/version.Version=..."
var Version = "dev"

// String retourne une description lisible de la version courante.
func String() string {
	return "pulse-stream " + Version
}
