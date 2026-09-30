package processor

import "testing"

func TestParseUserAgent(t *testing.T) {
	cases := []struct {
		name        string
		ua          string
		wantBrowser string
		wantDevice  string
		wantBot     bool
	}{
		{
			name:        "Chrome sur Mac",
			ua:          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
			wantBrowser: "chrome", wantDevice: DeviceDesktop,
		},
		{
			name:        "Edge (contient aussi Chrome et Safari)",
			ua:          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
			wantBrowser: "edge", wantDevice: DeviceDesktop,
		},
		{
			name:        "Firefox sur Linux",
			ua:          "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
			wantBrowser: "firefox", wantDevice: DeviceDesktop,
		},
		{
			name:        "Safari sur iPhone",
			ua:          "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
			wantBrowser: "safari", wantDevice: DeviceMobile,
		},
		{
			name:        "Safari sur iPad (contient aussi Mobile)",
			ua:          "Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
			wantBrowser: "safari", wantDevice: DeviceTablet,
		},
		{
			name:        "Chrome sur téléphone Android",
			ua:          "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
			wantBrowser: "chrome", wantDevice: DeviceMobile,
		},
		{
			name:        "Chrome sur tablette Android (sans Mobile)",
			ua:          "Mozilla/5.0 (Linux; Android 14; SM-X710) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
			wantBrowser: "chrome", wantDevice: DeviceTablet,
		},
		{
			name: "Googlebot", ua: "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
			wantBrowser: "other", wantDevice: DeviceBot, wantBot: true,
		},
		{
			name: "curl", ua: "curl/8.7.1",
			wantBrowser: "other", wantDevice: DeviceBot, wantBot: true,
		},
		{
			name: "Chrome sans interface", ua: "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/120.0.0.0 Safari/537.36",
			wantBrowser: "other", wantDevice: DeviceBot, wantBot: true,
		},
		{
			name: "user-agent absent", ua: "",
			wantBrowser: "other", wantDevice: DeviceBot, wantBot: true,
		},
		{
			name: "user-agent inconnu mais pas un robot", ua: "MonSuperNavigateur/1.0",
			wantBrowser: "other", wantDevice: DeviceDesktop,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseUserAgent(tc.ua)
			if got.Browser != tc.wantBrowser || got.Device != tc.wantDevice || got.Bot != tc.wantBot {
				t.Errorf("ParseUserAgent() = %+v, want browser=%s device=%s bot=%v",
					got, tc.wantBrowser, tc.wantDevice, tc.wantBot)
			}
		})
	}
}
