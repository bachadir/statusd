package admin

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

type Role int

const (
	RoleNone Role = iota
	RoleViewer
	RoleEditor
	RoleAdmin
)

func (r Role) String() string {
	switch r {
	case RoleViewer:
		return "viewer"
	case RoleEditor:
		return "editor"
	case RoleAdmin:
		return "admin"
	}
	return "none"
}

type OIDCConfig struct {
	Issuer           string   `yaml:"issuer"` // e.g. https://idp.example.com/application/o/statusd/
	ClientID         string   `yaml:"client_id"`
	ClientSecretFile string   `yaml:"client_secret_file"`
	ClientSecretEnv  string   `yaml:"client_secret_env"`
	Scopes           []string `yaml:"scopes"`        // default openid profile email groups
	GroupsClaim      string   `yaml:"groups_claim"`  // default "groups"
	ViewerGroups     []string `yaml:"viewer_groups"` // default status-viewer
	EditorGroups     []string `yaml:"editor_groups"` // default status-editor
	AdminGroups      []string `yaml:"admin_groups"`  // default status-admin
}

type Config struct {
	BaseURL        string     `yaml:"base_url"` // external URL of the panel, for the OIDC redirect
	SessionKeyFile string     `yaml:"session_key_file"`
	SessionKeyEnv  string     `yaml:"session_key_env"`
	SessionTTL     string     `yaml:"session_ttl"` // default 12h
	OIDC           OIDCConfig `yaml:"oidc"`
	GrafanaURL     string     `yaml:"grafana_url"` // optional, for signal links
	// DevInsecureUser disables OIDC and logs everyone in as this admin user.
	// Only for local development; refused when oidc.issuer is set.
	DevInsecureUser string `yaml:"dev_insecure_user"`
}

type Session struct {
	Sub   string `json:"sub"`
	Name  string `json:"name"`
	Email string `json:"email"`
	Role  Role   `json:"role"`
	Exp   int64  `json:"exp"`
	CSRF  string `json:"csrf"`
}

func (s *Session) Actor() string {
	if s.Email != "" {
		return s.Email
	}
	if s.Name != "" {
		return s.Name
	}
	return s.Sub
}

type auth struct {
	cfg      Config
	key      []byte
	ttl      time.Duration
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
	oauth    *oauth2.Config
	secure   bool
}

const (
	sessionCookie = "statusd_session"
	stateCookie   = "statusd_oidc"
)

func newAuth(ctx context.Context, cfg Config) (*auth, error) {
	a := &auth{cfg: cfg, ttl: 12 * time.Hour, secure: strings.HasPrefix(cfg.BaseURL, "https://")}
	if cfg.SessionTTL != "" {
		d, err := time.ParseDuration(cfg.SessionTTL)
		if err != nil {
			return nil, fmt.Errorf("session_ttl: %w", err)
		}
		a.ttl = d
	}
	switch {
	case cfg.SessionKeyFile != "":
		b, err := os.ReadFile(cfg.SessionKeyFile)
		if err != nil {
			return nil, err
		}
		a.key = []byte(strings.TrimSpace(string(b)))
	case cfg.SessionKeyEnv != "":
		a.key = []byte(os.Getenv(cfg.SessionKeyEnv))
	}
	if cfg.DevInsecureUser != "" {
		if cfg.OIDC.Issuer != "" {
			return nil, errors.New("dev_insecure_user cannot be combined with oidc.issuer")
		}
		if len(a.key) == 0 {
			a.key = randomBytes(32)
		}
		return a, nil
	}
	if cfg.OIDC.Issuer == "" {
		return nil, errors.New("admin.oidc.issuer is required (or dev_insecure_user for local development)")
	}
	if len(a.key) < 32 {
		return nil, errors.New("admin session key must be at least 32 bytes and identical on every node")
	}
	secret := ""
	switch {
	case cfg.OIDC.ClientSecretFile != "":
		b, err := os.ReadFile(cfg.OIDC.ClientSecretFile)
		if err != nil {
			return nil, err
		}
		secret = strings.TrimSpace(string(b))
	case cfg.OIDC.ClientSecretEnv != "":
		secret = os.Getenv(cfg.OIDC.ClientSecretEnv)
	}
	if cfg.BaseURL == "" {
		return nil, errors.New("admin.base_url is required for OIDC")
	}
	pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	p, err := oidc.NewProvider(pctx, cfg.OIDC.Issuer)
	if err != nil {
		return nil, fmt.Errorf("oidc discovery: %w", err)
	}
	scopes := cfg.OIDC.Scopes
	if len(scopes) == 0 {
		scopes = []string{oidc.ScopeOpenID, "profile", "email", "groups"}
	}
	a.provider = p
	a.verifier = p.Verifier(&oidc.Config{ClientID: cfg.OIDC.ClientID})
	a.oauth = &oauth2.Config{
		ClientID: cfg.OIDC.ClientID, ClientSecret: secret, Endpoint: p.Endpoint(),
		RedirectURL: strings.TrimRight(cfg.BaseURL, "/") + "/auth/callback", Scopes: scopes,
	}
	return a, nil
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

func randomString(n int) string { return base64.RawURLEncoding.EncodeToString(randomBytes(n)) }

func (a *auth) sign(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	payload := base64.RawURLEncoding.EncodeToString(b)
	m := hmac.New(sha256.New, a.key)
	m.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil)), nil
}

func (a *auth) verify(tok string, v any) error {
	payload, sig, ok := strings.Cut(tok, ".")
	if !ok {
		return errors.New("malformed token")
	}
	m := hmac.New(sha256.New, a.key)
	m.Write([]byte(payload))
	want := m.Sum(nil)
	got, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil || !hmac.Equal(got, want) {
		return errors.New("bad signature")
	}
	b, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func (a *auth) setCookie(w http.ResponseWriter, name, value string, maxAge time.Duration) {
	http.SetCookie(w, &http.Cookie{
		Name: name, Value: value, Path: "/", HttpOnly: true, Secure: a.secure,
		SameSite: http.SameSiteLaxMode, MaxAge: int(maxAge.Seconds()),
	})
}

// session returns the current user, or nil.
func (a *auth) session(r *http.Request) *Session {
	if a.cfg.DevInsecureUser != "" {
		return &Session{Sub: a.cfg.DevInsecureUser, Name: a.cfg.DevInsecureUser, Email: a.cfg.DevInsecureUser, Role: RoleAdmin, CSRF: devCSRF(a.key)}
	}
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return nil
	}
	var s Session
	if err := a.verify(c.Value, &s); err != nil || time.Now().Unix() > s.Exp || s.Role == RoleNone {
		return nil
	}
	return &s
}

func devCSRF(key []byte) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte("dev-csrf"))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

type oidcState struct {
	State    string `json:"state"`
	Nonce    string `json:"nonce"`
	Verifier string `json:"verifier"`
	Return   string `json:"return"`
	Exp      int64  `json:"exp"`
}

func (a *auth) login(w http.ResponseWriter, r *http.Request) {
	if a.cfg.DevInsecureUser != "" {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	ret := r.URL.Query().Get("return")
	if !strings.HasPrefix(ret, "/") || strings.HasPrefix(ret, "//") {
		ret = "/"
	}
	st := oidcState{State: randomString(16), Nonce: randomString(16), Verifier: oauth2.GenerateVerifier(), Return: ret, Exp: time.Now().Add(10 * time.Minute).Unix()}
	tok, err := a.sign(st)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.setCookie(w, stateCookie, tok, 10*time.Minute)
	url := a.oauth.AuthCodeURL(st.State, oidc.Nonce(st.Nonce), oauth2.S256ChallengeOption(st.Verifier))
	http.Redirect(w, r, url, http.StatusFound)
}

func (a *auth) callback(w http.ResponseWriter, r *http.Request) {
	c, err := r.Cookie(stateCookie)
	if err != nil {
		http.Error(w, "login expired, please retry", http.StatusBadRequest)
		return
	}
	var st oidcState
	if err := a.verify(c.Value, &st); err != nil || time.Now().Unix() > st.Exp || r.URL.Query().Get("state") != st.State {
		http.Error(w, "invalid login state, please retry", http.StatusBadRequest)
		return
	}
	a.setCookie(w, stateCookie, "", -time.Second)
	if e := r.URL.Query().Get("error"); e != "" {
		http.Error(w, "login failed: "+e, http.StatusUnauthorized)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	tok, err := a.oauth.Exchange(ctx, r.URL.Query().Get("code"), oauth2.VerifierOption(st.Verifier))
	if err != nil {
		http.Error(w, "token exchange failed", http.StatusUnauthorized)
		return
	}
	rawID, ok := tok.Extra("id_token").(string)
	if !ok {
		http.Error(w, "no id_token in response", http.StatusUnauthorized)
		return
	}
	idt, err := a.verifier.Verify(ctx, rawID)
	if err != nil || idt.Nonce != st.Nonce {
		http.Error(w, "invalid id_token", http.StatusUnauthorized)
		return
	}
	claims := map[string]any{}
	if err := idt.Claims(&claims); err != nil {
		http.Error(w, "bad claims", http.StatusUnauthorized)
		return
	}
	role := a.roleFor(claims)
	if role == RoleNone {
		http.Error(w, "your account is not in any status page group", http.StatusForbidden)
		return
	}
	sess := Session{Sub: idt.Subject, Role: role, Exp: time.Now().Add(a.ttl).Unix(), CSRF: randomString(24)}
	sess.Name, _ = claims["name"].(string)
	if sess.Name == "" {
		sess.Name, _ = claims["preferred_username"].(string)
	}
	sess.Email, _ = claims["email"].(string)
	v, err := a.sign(sess)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.setCookie(w, sessionCookie, v, a.ttl)
	http.Redirect(w, r, st.Return, http.StatusFound)
}

func (a *auth) roleFor(claims map[string]any) Role {
	gc := a.cfg.OIDC.GroupsClaim
	if gc == "" {
		gc = "groups"
	}
	var groups []string
	switch v := claims[gc].(type) {
	case []any:
		for _, g := range v {
			if s, ok := g.(string); ok {
				groups = append(groups, s)
			}
		}
	case string:
		groups = []string{v}
	}
	def := func(v []string, d string) []string {
		if len(v) == 0 {
			return []string{d}
		}
		return v
	}
	has := func(want []string) bool {
		for _, g := range groups {
			if slices.Contains(want, g) {
				return true
			}
		}
		return false
	}
	switch {
	case has(def(a.cfg.OIDC.AdminGroups, "status-admin")):
		return RoleAdmin
	case has(def(a.cfg.OIDC.EditorGroups, "status-editor")):
		return RoleEditor
	case has(def(a.cfg.OIDC.ViewerGroups, "status-viewer")):
		return RoleViewer
	}
	return RoleNone
}

func (a *auth) logout(w http.ResponseWriter, r *http.Request) {
	a.setCookie(w, sessionCookie, "", -time.Second)
	if a.provider != nil {
		var meta struct {
			EndSession string `json:"end_session_endpoint"`
		}
		if err := a.provider.Claims(&meta); err == nil && meta.EndSession != "" {
			http.Redirect(w, r, meta.EndSession, http.StatusFound)
			return
		}
	}
	http.Redirect(w, r, "/", http.StatusFound)
}
