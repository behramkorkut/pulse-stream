// Commande collector : point d'entrée HTTP de la chaîne (palier 0 : simple vérification de l'outillage).
package main

import (
	"fmt"

	"github.com/behramkorkut/pulse-stream/internal/version"
)

func main() {
	fmt.Println(version.String())
}
