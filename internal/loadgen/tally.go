package loadgen

// Totals regroupe les trois compteurs que l'aggregator tient dans MongoDB.
type Totals struct {
	Pageviews int64
	Clicks    int64
	Bots      int64
}

// Tally retient les événements que le collector a ACCEPTÉS (202), une fois chacun : c'est ce que le
// pipeline doit finir par compter, ni plus (doublon compté deux fois) ni moins (événement perdu).
type Tally map[string]ackInfo

type ackInfo struct {
	typ     string
	bot     bool
	tooLate bool // accepté, mais trop en retard : attendu en dead-letter, pas dans les compteurs
}

// Add enregistre une requête et la réponse du collector. Seuls les nouveaux événements acceptés comptent ;
// un doublon accepté porte le même identifiant : il retombe sur la même entrée.
func (t Tally) Add(r Request, status int) {
	if status != 202 || r.Kind == KindInvalid || r.ID == "" {
		return
	}
	t[r.ID] = ackInfo{typ: r.Type, bot: r.Bot, tooLate: r.TooLate}
}

// Merge ajoute les entrées de other.
func (t Tally) Merge(other Tally) {
	for id, info := range other {
		t[id] = info
	}
}

// Totals convertit le bilan en compteurs attendus : les robots comptent à part, jamais comme pageviews ou clics,
// et les événements trop en retard ne comptent pas du tout.
func (t Tally) Totals() Totals {
	var out Totals
	for _, info := range t {
		switch {
		case info.tooLate:
			continue
		case info.bot:
			out.Bots++
		case info.typ == "click":
			out.Clicks++
		default:
			out.Pageviews++
		}
	}
	return out
}

// TooLate compte les événements acceptés par le collector mais trop en retard : le processor les met en
// dead-letter, MongoDB ne doit pas les compter.
func (t Tally) TooLate() int {
	n := 0
	for _, info := range t {
		if info.tooLate {
			n++
		}
	}
	return n
}
