package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

type apiConfig struct {
	listenAddr string
	dbPath     string
	password   string
}

type apiServer struct {
	store *storage
	info  serviceInfo
}

type serviceInfo struct {
	ListenAddr    string   `json:"listen_addr"`
	LocalURLs     []string `json:"local_urls"`
	TailscaleURLs []string `json:"tailscale_urls"`
}

func startAPIServer(cfg apiConfig) error {
	store, err := openStorage(cfg.dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	info := buildServiceInfo(cfg.listenAddr)
	for _, url := range info.LocalURLs {
		fmt.Printf("serve local: %s\n", url)
	}
	for _, url := range info.TailscaleURLs {
		fmt.Printf("serve tailscale: %s\n", url)
	}
	if cfg.password != "" {
		fmt.Printf("serve auth: request password enabled\n")
	}

	server := &apiServer{store: store, info: info}
	httpServer := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           withRequestPassword(server.routes(), cfg.password),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return fmt.Errorf("serve http api: %w", httpServer.ListenAndServe())
}

func (s *apiServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/nodes/", s.handleNodeRoutes)
	mux.HandleFunc("/neighbors/", s.handleNeighborCount)
	mux.HandleFunc("/ip/", s.handleIPLookup)
	mux.HandleFunc("/subnet/", s.handleSubnetLookup)
	return mux
}

func (s *apiServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/health" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"service": s.info,
	})
}

func (s *apiServer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/stats" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	stats, err := s.store.getStats(time.Now())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":    true,
		"stats": stats,
	})
}

func (s *apiServer) handleNodeRoutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/nodes/")
	path = strings.Trim(path, "/")
	if path == "" {
		writeError(w, http.StatusNotFound, "node id is required")
		return
	}

	parts := strings.Split(path, "/")
	nodeID := parts[0]

	if len(parts) == 1 {
		rec, err := s.store.getNode(nodeID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if rec == nil {
			writeError(w, http.StatusNotFound, "node not found")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":   true,
			"node": rec,
		})
		return
	}

	if len(parts) == 2 && parts[1] == "history" {
		history, err := s.store.getNodeHistory(nodeID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":      true,
			"node_id": nodeID,
			"count":   len(history),
			"history": history,
		})
		return
	}

	writeError(w, http.StatusNotFound, "not found")
}

func (s *apiServer) handleIPLookup(w http.ResponseWriter, r *http.Request) {
	rawIP := strings.Trim(strings.TrimPrefix(r.URL.Path, "/ip/"), "/")
	parsed := net.ParseIP(rawIP)
	if parsed == nil {
		writeError(w, http.StatusBadRequest, "invalid ip")
		return
	}
	ip := parsed.String()

	subnet24, err := ipv4Subnet24(ip)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	nodesByIP, err := s.store.getNodesByIP(ip)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	subnetNodes, err := s.store.getNodesBySubnet(subnet24)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                    true,
		"ip":                    ip,
		"subnet_24":             subnet24,
		"matching_nodes_count":  len(nodesByIP),
		"matching_nodes":        nodesByIP,
		"subnet_nodes_count":    len(subnetNodes),
		"subnet_neighbor_count": len(subnetNodes),
		"subnet_nodes":          subnetNodes,
	})
}

func (s *apiServer) handleNeighborCount(w http.ResponseWriter, r *http.Request) {
	rawIP := strings.Trim(strings.TrimPrefix(r.URL.Path, "/neighbors/"), "/")
	parsed := net.ParseIP(rawIP)
	if parsed == nil {
		http.Error(w, "invalid ip", http.StatusBadRequest)
		return
	}
	ip := parsed.String()

	subnet24, err := ipv4Subnet24(ip)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	subnetNodes, err := s.store.getNodesBySubnet(subnet24)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, "%d\n", len(subnetNodes))
}

func (s *apiServer) handleSubnetLookup(w http.ResponseWriter, r *http.Request) {
	rawSubnet := strings.Trim(strings.TrimPrefix(r.URL.Path, "/subnet/"), "/")
	subnet24, err := normalizeSubnet24(rawSubnet)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	subnetNodes, err := s.store.getNodesBySubnet(subnet24)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                 true,
		"subnet_24":          subnet24,
		"subnet_nodes_count": len(subnetNodes),
		"subnet_nodes":       subnetNodes,
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"ok":    false,
		"error": message,
	})
}

func withRequestPassword(next http.Handler, password string) http.Handler {
	if password == "" {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		provided := r.Header.Get("X-API-Password")
		if provided == "" {
			provided = r.URL.Query().Get("password")
		}
		if !secureEquals(provided, password) {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func secureEquals(left, right string) bool {
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func buildServiceInfo(listenAddr string) serviceInfo {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		host = ""
		port = "8080"
	}

	info := serviceInfo{
		ListenAddr: listenAddr,
	}

	switch host {
	case "", "0.0.0.0":
		info.LocalURLs = append(info.LocalURLs, "http://0.0.0.0:"+port)
		info.LocalURLs = append(info.LocalURLs, "http://127.0.0.1:"+port)
	case "::", "[::]":
		info.LocalURLs = append(info.LocalURLs, "http://[::1]:"+port)
	default:
		info.LocalURLs = append(info.LocalURLs, "http://"+host+":"+port)
	}

	for _, ip := range tailscaleIPv4s() {
		info.TailscaleURLs = append(info.TailscaleURLs, "http://"+ip+":"+port)
	}

	return info
}

func tailscaleIPv4s() []string {
	iface, err := net.InterfaceByName("tailscale0")
	if err != nil {
		return nil
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return nil
	}

	seen := map[string]struct{}{}
	var ips []string
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil {
			continue
		}
		v4 := ip.To4()
		if v4 == nil {
			continue
		}
		value := v4.String()
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		ips = append(ips, value)
	}
	return ips
}
