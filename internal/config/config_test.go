package config

import "testing"

func TestCleanURL(t *testing.T) {
	for in, want := range map[string]string{
		"  rediss://u:p@h:6379\n": "rediss://u:p@h:6379",
		`"postgresql://u:p@h/db"`: "postgresql://u:p@h/db",
		"'redis://h:6379'":        "redis://h:6379",
	} {
		if got := cleanURL(in); got != want {
			t.Errorf("cleanURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPortAndRenderURL(t *testing.T) {
	t.Setenv("PORT", "10000")
	t.Setenv("RENDER_EXTERNAL_URL", "https://x.onrender.com/")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":10000" || c.BaseURL != "https://x.onrender.com" {
		t.Fatalf("got addr %q base %q", c.HTTPAddr, c.BaseURL)
	}
}
