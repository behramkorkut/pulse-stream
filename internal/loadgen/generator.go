// Package loadgen produit une charge synthétique contre le collector et en mesure l'effet.
//
// Trois idées structurent le paquet :
//   - un générateur déterministe (même graine, mêmes événements) qui sait ce qu'il envoie ;
//   - un exécuteur "à débit imposé" (open-loop) qui mesure la latence depuis l'instant où la requête
//     DEVAIT partir, pas depuis celui où elle est partie (sinon un serveur lent masque sa propre lenteur) ;
//   - un vérificateur qui compare ce que le générateur a fait accepter à ce que MongoDB a fini par compter.
package loadgen

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// Kind distingue la nature d'une requête générée.
type Kind int

const (
	KindValid     Kind = iota // un nouvel événement valide
	KindDuplicate             // un événement déjà envoyé, renvoyé à l'identique
	KindInvalid               // un corps que le collector doit refuser (400 ou 422)
)

// Mix règle la composition du trafic. Chaque champ est une proportion entre 0 et 1.
type Mix struct {
	Duplicate float64 // part des requêtes qui renvoient un événement déjà envoyé
	Invalid   float64 // part des requêtes refusées par le collector
	Bot       float64 // part des événements valides émis par des robots
	Click     float64 // part des événements valides qui sont des clics (les autres : pageviews)
}

// GenConfig règle le générateur.
type GenConfig struct {
	RunID    string // identifie l'exécution ; préfixe les sites et les identifiants
	Sites    int    // nombre de sites distincts (défaut : 3)
	Visitors int    // nombre de visiteurs distincts par site (défaut : 10 000)
	Mix      Mix
	Seed     uint64
}

// Request est une requête HTTP prête à partir, avec ce qu'il faut savoir pour la comptabiliser ensuite.
type Request struct {
	Body []byte
	Kind Kind
	ID   string // identifiant de l'événement ; vide pour KindInvalid
	Type string // pageview | click ; vide pour KindInvalid
	Bot  bool
}

// recentWindow : un doublon rejoue l'un des derniers événements valides, comme un client qui réessaie.
const recentWindow = 128

var (
	humanAgents = []string{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
		"Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
	}
	botAgent = "Googlebot/2.1 (+http://www.google.com/bot.html)"
)

// Generator fabrique les requêtes. Il n'est PAS sûr pour un usage concurrent : une seule goroutine l'appelle.
type Generator struct {
	cfg    GenConfig
	rng    *rand.Rand
	seq    int
	recent []Request
	cursor int
}

// NewGenerator crée un générateur avec des valeurs par défaut raisonnables.
func NewGenerator(cfg GenConfig) *Generator {
	if cfg.Sites <= 0 {
		cfg.Sites = 3
	}
	if cfg.Visitors <= 0 {
		cfg.Visitors = 10_000
	}
	if cfg.RunID == "" {
		cfg.RunID = "run"
	}
	return &Generator{cfg: cfg, rng: rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))}
}

// SitePrefix est le début commun de tous les identifiants de site de cette exécution.
// Il permet de retrouver, dans MongoDB, exactement ce que cette charge a produit.
func (g *Generator) SitePrefix() string { return "load-" + g.cfg.RunID + "-" }

// Next fabrique la requête suivante. now est l'horodatage porté par les nouveaux événements.
func (g *Generator) Next(now time.Time) Request {
	roll := g.rng.Float64()
	switch {
	case roll < g.cfg.Mix.Invalid:
		return g.invalid()
	case roll < g.cfg.Mix.Invalid+g.cfg.Mix.Duplicate && len(g.recent) > 0:
		r := g.recent[g.rng.IntN(len(g.recent))]
		r.Kind = KindDuplicate
		return r
	}
	return g.valid(now)
}

func (g *Generator) valid(now time.Time) Request {
	g.seq++
	id := fmt.Sprintf("load-%s-%d", g.cfg.RunID, g.seq)
	site := fmt.Sprintf("%ss%d", g.SitePrefix(), g.rng.IntN(g.cfg.Sites))
	visitor := fmt.Sprintf("v-%d", g.rng.IntN(g.cfg.Visitors))

	typ := "pageview"
	if g.rng.Float64() < g.cfg.Mix.Click {
		typ = "click"
	}
	bot := g.rng.Float64() < g.cfg.Mix.Bot
	agent := botAgent
	if !bot {
		agent = humanAgents[g.rng.IntN(len(humanAgents))]
	}

	body := fmt.Appendf(nil,
		`{"id":%q,"type":%q,"site_id":%q,"visitor_id":%q,"url":"https://example.com/page-%d","user_agent":%q,"timestamp":%q}`,
		id, typ, site, visitor, g.rng.IntN(50), agent, now.UTC().Format(time.RFC3339))

	r := Request{Body: body, Kind: KindValid, ID: id, Type: typ, Bot: bot}
	g.remember(r)
	return r
}

func (g *Generator) remember(r Request) {
	if len(g.recent) < recentWindow {
		g.recent = append(g.recent, r)
		return
	}
	g.recent[g.cursor] = r
	g.cursor = (g.cursor + 1) % recentWindow
}

// invalid alterne entre un JSON cassé (400) et un événement incomplet (422).
func (g *Generator) invalid() Request {
	if g.rng.IntN(2) == 0 {
		return Request{Body: []byte(`{oops`), Kind: KindInvalid}
	}
	return Request{Body: []byte(`{"id":"incomplet"}`), Kind: KindInvalid}
}
