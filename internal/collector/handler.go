// Package collector contient le serveur HTTP qui reçoit les événements d'analytics.
package collector

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

// maxBodyBytes limite la taille d'un corps de requête : un événement fait quelques centaines
// d'octets, 64 Kio est très large et protège le serveur contre les corps démesurés.
const maxBodyBytes = 64 << 10

type api struct {
	pub     Publisher
	log     *slog.Logger
	now     func() time.Time // injectable pour les tests
	metrics *Metrics         // nil : aucune mesure
}

// NewHandler construit le routeur HTTP du collector. m peut être nil (aucune mesure).
func NewHandler(pub Publisher, log *slog.Logger, m *Metrics) http.Handler {
	return newHandler(pub, log, time.Now, m)
}

func newHandler(pub Publisher, log *slog.Logger, now func() time.Time, m *Metrics) http.Handler {
	a := &api{pub: pub, log: log, now: now, metrics: m}

	mux := http.NewServeMux()
	// Depuis Go 1.22, le motif peut contenir la méthode HTTP : toute autre méthode reçoit un 405.
	mux.HandleFunc("POST /collect", m.instrument(a.collect))
	mux.HandleFunc("GET /healthz", a.healthz)
	return mux
}

func (a *api) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *api) collect(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields() // un champ inconnu est probablement une faute de frappe côté client

	var e event.Event
	if err := dec.Decode(&e); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	// Ces champs appartiennent au serveur : on écrase ce que le client a pu envoyer.
	e.ReceivedAt = a.now().UTC()
	e.IP = clientIP(r)
	if e.UserAgent == "" {
		e.UserAgent = r.UserAgent()
	}

	if problems := e.Validate(e.ReceivedAt); len(problems) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"errors": problems})
		return
	}

	publishStart := time.Now()
	err := a.pub.Publish(r.Context(), e)
	a.metrics.observePublish(time.Since(publishStart))
	if err != nil {
		a.log.ErrorContext(r.Context(), "publish failed", slog.String("id", e.ID), slog.Any("error", err))
		writeError(w, http.StatusServiceUnavailable, "temporarily unavailable, retry later")
		return
	}

	// 202 Accepted : l'événement est pris en charge, son traitement est asynchrone.
	writeJSON(w, http.StatusAccepted, map[string]string{"id": e.ID})
}

// clientIP retourne l'adresse IP de l'appelant. On ne fait volontairement pas confiance à
// X-Forwarded-For pour l'instant : n'importe quel client peut l'écrire. On y reviendra
// quand un reverse proxy sera devant le collector.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // le statut est déjà envoyé : une erreur d'écriture n'est plus récupérable
}
