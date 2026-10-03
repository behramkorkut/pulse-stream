package dedupe

import "testing"

func TestRedisKeyIsReadableAndUnambiguous(t *testing.T) {
	if got, want := redisKey(Key{SiteID: "site-42", EventID: "evt-1"}), "pulse:seen:7:site-42:evt-1"; got != want {
		t.Errorf("redisKey() = %q, want %q", got, want)
	}
	// Avec un simple séparateur, ces deux événements partageraient la clé "pulse:seen:a:b:c".
	if a, b := redisKey(Key{SiteID: "a:b", EventID: "c"}), redisKey(Key{SiteID: "a", EventID: "b:c"}); a == b {
		t.Errorf("deux événements différents ont la même clé %q", a)
	}
}
