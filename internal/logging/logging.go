// Package logging construit le journal JSON des programmes, avec un niveau réglable par variable d'environnement.
package logging

import (
	"fmt"
	"io"
	"log/slog"
)

// New crée un journal JSON écrit dans w. level vaut debug, info, warn ou error (insensible à la casse) ;
// vide : info. Une valeur inconnue est une erreur : mieux vaut refuser de démarrer que journaliser
// au mauvais niveau sans le savoir.
//
// Le niveau sert à doser le bruit. Les lignes "un lot traité" sont au niveau debug : à des milliers de
// lots par seconde, les écrire coûte du temps et noie l'information utile ; les métriques les remplacent.
func New(w io.Writer, level string) (*slog.Logger, error) {
	lvl := slog.LevelInfo
	if level != "" {
		if err := lvl.UnmarshalText([]byte(level)); err != nil {
			return nil, fmt.Errorf("invalid LOG_LEVEL %q (expected debug, info, warn or error)", level)
		}
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})), nil
}
