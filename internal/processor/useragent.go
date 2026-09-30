// Package processor lit les événements bruts dans Kafka, les valide, les enrichit
// et publie le résultat (ou le rejet) dans les topics de sortie.
package processor

import "strings"

// Valeurs possibles du champ Device.
const (
	DeviceDesktop = "desktop"
	DeviceMobile  = "mobile"
	DeviceTablet  = "tablet"
	DeviceBot     = "bot"
)

// UserAgentInfo est le résultat de l'analyse d'un user-agent.
type UserAgentInfo struct {
	Browser string
	Device  string
	Bot     bool
}

// botMarkers : fragments (en minuscules) qui trahissent du trafic automatisé.
// C'est une heuristique : elle attrape les robots honnêtes qui s'annoncent, pas les malveillants.
var botMarkers = []string{
	"bot", "crawl", "spider", "slurp",
	"curl/", "wget/", "python-requests", "python-urllib", "go-http-client", "httpclient",
	"headlesschrome", "phantomjs", "lighthouse", "facebookexternalhit",
}

// ParseUserAgent classe un user-agent : robot ou non, navigateur, type d'appareil.
//
// L'ordre des tests compte : les user-agents mentent par héritage. Edge et Opera contiennent
// aussi "Chrome" et "Safari", Chrome contient aussi "Safari" : on teste du plus spécifique
// au plus générique.
func ParseUserAgent(ua string) UserAgentInfo {
	lower := strings.ToLower(strings.TrimSpace(ua))

	// Un navigateur réel s'annonce toujours : l'absence de user-agent est suspecte.
	if lower == "" || containsAny(lower, botMarkers) {
		return UserAgentInfo{Browser: "other", Device: DeviceBot, Bot: true}
	}

	return UserAgentInfo{Browser: browserOf(lower), Device: deviceOf(lower)}
}

func browserOf(lower string) string {
	switch {
	case containsAny(lower, []string{"edg/", "edge/", "edga/", "edgios/"}):
		return "edge"
	case containsAny(lower, []string{"opr/", "opera"}):
		return "opera"
	case containsAny(lower, []string{"firefox/", "fxios/"}):
		return "firefox"
	case containsAny(lower, []string{"chrome/", "crios/"}):
		return "chrome"
	case strings.Contains(lower, "safari/"):
		return "safari"
	default:
		return "other"
	}
}

func deviceOf(lower string) string {
	switch {
	case strings.Contains(lower, "ipad") || strings.Contains(lower, "tablet"):
		return DeviceTablet
	// Android sans le mot "Mobile" désigne une tablette.
	case strings.Contains(lower, "android") && !strings.Contains(lower, "mobile"):
		return DeviceTablet
	case containsAny(lower, []string{"mobile", "iphone", "android"}):
		return DeviceMobile
	default:
		return DeviceDesktop
	}
}

func containsAny(s string, fragments []string) bool {
	for _, f := range fragments {
		if strings.Contains(s, f) {
			return true
		}
	}
	return false
}
