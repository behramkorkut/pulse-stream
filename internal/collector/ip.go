package collector

import "net/netip"

// Bits conservés : le dernier octet d'une IPv4 (/24) et tout ce qui suit les 48 premiers bits d'une IPv6
// (/48) sont mis à zéro. C'est la troncature de l'option anonymize_ip de Google Analytics : elle garde une
// localisation grossière (pays, opérateur) et ne désigne plus une machine précise.
const (
	keepBitsIPv4 = 24
	keepBitsIPv6 = 48
)

// anonymizeIP tronque une adresse IP avant qu'elle quitte le collector (RGPD : minimisation des données).
// L'adresse complète n'est jamais publiée dans Kafka, donc jamais stockée.
//
// Une IPv4 transportée en IPv6 (::ffff:203.0.113.42) est traitée comme une IPv4. Une valeur illisible donne
// une chaîne vide : mieux vaut ne rien garder que recopier une valeur brute qu'on ne sait pas tronquer.
func anonymizeIP(addr string) string {
	ip, err := netip.ParseAddr(addr)
	if err != nil {
		return ""
	}
	ip = ip.Unmap()

	bits := keepBitsIPv6
	if ip.Is4() {
		bits = keepBitsIPv4
	}
	prefix, err := ip.Prefix(bits)
	if err != nil {
		return ""
	}
	return prefix.Addr().String()
}
