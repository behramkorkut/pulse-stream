// Commande collector : point d'entrée HTTP de la chaîne d'analytics.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/collector"
	"github.com/behramkorkut/pulse-stream/internal/version"
)

func main() {
	if err := run(); err != nil {
		slog.Error("collector stopped with an error", slog.Any("error", err))
		os.Exit(1)
	}
}

// run contient toute la logique et retourne une erreur : main() est le seul endroit qui appelle
// os.Exit, ce qui garantit que les `defer` ont bien le temps de s'exécuter.
func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	addr := getenv("COLLECTOR_ADDR", ":8080")

	// ctx est annulé quand le processus reçoit Ctrl+C (SIGINT) ou SIGTERM (envoyé par Docker/Kubernetes).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:    addr,
		Handler: collector.NewHandler(collector.NewLogPublisher(log), log),
		// Sans ces délais, un client lent ou malveillant peut garder une connexion ouverte indéfiniment.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Le serveur tourne dans une goroutine pour que main puisse attendre le signal d'arrêt en parallèle.
	// Le canal est tamponné (1) : la goroutine ne reste jamais bloquée si personne ne lit son résultat.
	errCh := make(chan error, 1)
	go func() {
		log.Info("collector listening", slog.String("addr", addr), slog.String("version", version.Version))
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("server failed: %w", err) // port déjà pris, par exemple
	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	// Arrêt propre : on cesse d'accepter de nouvelles connexions et on laisse 10 s
	// aux requêtes en cours pour se terminer.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("graceful shutdown: %w", err)
	}

	log.Info("collector stopped cleanly")
	return nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
