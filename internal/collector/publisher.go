package collector

import (
	"context"
	"log/slog"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

// Publisher est la destination des événements acceptés.
//
// C'est une interface : le handler HTTP ne sait pas si les événements partent dans
// Kafka, dans un fichier ou dans un test. Au palier 2, on branchera Kafka derrière
// cette même interface sans toucher au handler.
type Publisher interface {
	Publish(ctx context.Context, e event.Event) error
}

// LogPublisher se contente de journaliser chaque événement. Implémentation provisoire du palier 1.
type LogPublisher struct {
	log *slog.Logger
}

// NewLogPublisher crée un LogPublisher.
func NewLogPublisher(log *slog.Logger) *LogPublisher {
	return &LogPublisher{log: log}
}

// Publish journalise l'événement et ne peut pas échouer.
func (p *LogPublisher) Publish(ctx context.Context, e event.Event) error {
	p.log.LogAttrs(ctx, slog.LevelInfo, "event accepted",
		slog.String("id", e.ID),
		slog.String("type", e.Type),
		slog.String("site_id", e.SiteID),
	)
	return nil
}
