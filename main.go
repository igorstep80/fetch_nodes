package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/net/proxy"
	"storj.io/common/identity"
	"storj.io/common/macaroon"
	"storj.io/common/pb"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
)

type fetchConfig struct {
	satellite string
	apiKey    string
	bucket    string
	objectKey string
	dbPath    string
	socks5    string
	timeout   time.Duration
	pretty    bool
}

type runtimeConfig struct {
	dbPath      string
	updateEvery time.Duration
	printJSON   bool
}

type fetchResult struct {
	Satellite  string                 `json:"satellite,omitempty"`
	FetchedAt  string                 `json:"fetched_at"`
	Count      int                    `json:"count"`
	Nodes      []node                 `json:"nodes"`
	Satellites []satelliteFetchResult `json:"satellites,omitempty"`
	Summary    *fetchRunSummary       `json:"summary,omitempty"`
}

type node struct {
	NodeID     string   `json:"node_id"`
	Address    string   `json:"address"`
	IP         string   `json:"ip"`
	Port       int      `json:"port"`
	Satellites []string `json:"satellites,omitempty"`
}

type satelliteConfig struct {
	Name      string
	Satellite string
	APIKey    string
}

type satelliteFetchResult struct {
	Name      string `json:"name,omitempty"`
	Satellite string `json:"satellite"`
	Count     int    `json:"count"`
}

type fetchRunSummary struct {
	SatellitesCount    int `json:"satellites_count"`
	FetchedNodes       int `json:"fetched_nodes"`
	NewNodes           int `json:"new_nodes"`
	NewIPs             int `json:"new_ips"`
	UpdatedNodes       int `json:"updated_nodes"`
	ChangedNodes       int `json:"changed_nodes"`
	UnchangedNodes     int `json:"unchanged_nodes"`
	DatabaseTotalNodes int `json:"database_total_nodes"`
	DatabaseTotalIPs   int `json:"database_total_ips"`
}

func main() {
	cfg := fetchConfig{}
	runtimeCfg := runtimeConfig{}
	apiCfg := apiConfig{}

	_ = loadDotEnv(".env")

	runFetch := func(cmd *cobra.Command, args []string) (fetchResult, error) {
		if cfg.bucket == "" {
			cfg.bucket = os.Getenv("STORJ_BUCKET")
		}
		if cfg.bucket == "" {
			return fetchResult{}, errors.New("bucket is required via --bucket or STORJ_BUCKET")
		}
		if cfg.objectKey == "" {
			cfg.objectKey = os.Getenv("STORJ_OBJECT_KEY")
		}
		if cfg.objectKey == "" {
			cfg.objectKey = "probe"
		}
		if cfg.timeout <= 0 {
			return fetchResult{}, errors.New("timeout must be greater than zero")
		}

		ctx, cancel := context.WithTimeout(cmd.Context(), cfg.timeout)
		defer cancel()

		satelliteConfigs, err := resolveSatelliteConfigs(cfg)
		if err != nil {
			return fetchResult{}, err
		}

		result, err := executeFetch(ctx, cfg, satelliteConfigs, runtimeCfg.dbPath)
		if err != nil {
			return fetchResult{}, err
		}
		if runtimeCfg.printJSON {
			if err := printFetchResult(result, cfg.pretty); err != nil {
				return fetchResult{}, err
			}
		}
		return result, nil
	}

	fetchCmd := &cobra.Command{
		Use:   "fetch_nodes",
		Short: "Fetch Storj storage nodes from a satellite",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := runFetch(cmd, args)
			return err
		},
	}

	watchCmd := &cobra.Command{
		Use:   "watch",
		Short: "Fetch nodes continuously on a fixed interval",
		RunE: func(cmd *cobra.Command, args []string) error {
			if runtimeCfg.updateEvery <= 0 {
				return errors.New("update interval must be greater than zero")
			}
			runtimeCfg.printJSON = false

			for {
				started := time.Now().UTC()
				result, err := runFetch(cmd, args)
				if err != nil {
					log.Printf("fetch failed at %s: %v", started.Format(time.RFC3339), err)
				} else {
					log.Printf("fetch completed at %s: %s", started.Format(time.RFC3339), formatWatchSummary(result))
				}

				select {
				case <-cmd.Context().Done():
					return cmd.Context().Err()
				case <-time.After(runtimeCfg.updateEvery):
				}
			}
		},
	}

	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve the local node database over HTTP",
		RunE: func(cmd *cobra.Command, args []string) error {
			if apiCfg.listenAddr == "" {
				apiCfg.listenAddr = os.Getenv("STORJ_API_LISTEN_ADDR")
			}
			if apiCfg.listenAddr == "" {
				apiCfg.listenAddr = "127.0.0.1:8080"
			}
			if apiCfg.password == "" {
				apiCfg.password = os.Getenv("STORJ_API_PASSWORD")
			}
			apiCfg.dbPath = runtimeCfg.dbPath
			return startAPIServer(apiCfg)
		},
	}

	fetchCmd.Flags().StringVar(&cfg.satellite, "satellite", "", "satellite node URL; defaults to STORJ_SATELLITE_ADDRESS")
	fetchCmd.Flags().StringVar(&cfg.apiKey, "api-key", "", "Storj API key; defaults to STORJ_API_KEY")
	fetchCmd.Flags().StringVar(&cfg.bucket, "bucket", "", "Storj bucket name; defaults to STORJ_BUCKET")
	fetchCmd.Flags().StringVar(&cfg.objectKey, "object-key", "", "dummy object key used for the segment request; defaults to STORJ_OBJECT_KEY or probe")
	fetchCmd.Flags().StringVar(&cfg.socks5, "socks5", "", "optional SOCKS5 proxy in address:port or address:port:user:password format")
	fetchCmd.Flags().DurationVar(&cfg.timeout, "timeout", 30*time.Second, "overall request timeout")
	fetchCmd.Flags().BoolVar(&cfg.pretty, "pretty", true, "pretty-print JSON output")
	watchCmd.Flags().AddFlagSet(fetchCmd.Flags())

	fetchCmd.PersistentFlags().StringVar(&runtimeCfg.dbPath, "db-path", "", "SQLite database path; defaults to STORJ_DB_PATH or nodes.db")
	fetchCmd.PersistentFlags().DurationVar(&runtimeCfg.updateEvery, "every", 10*time.Second, "watch interval; defaults to STORJ_UPDATE_EVERY or 10s")
	serveCmd.Flags().StringVar(&apiCfg.listenAddr, "listen", "", "HTTP listen address; defaults to STORJ_API_LISTEN_ADDR or 127.0.0.1:8080")
	serveCmd.Flags().StringVar(&apiCfg.password, "password", "", "request password; defaults to STORJ_API_PASSWORD")

	rootCmd := &cobra.Command{
		Use:   "fetch_nodes",
		Short: "Fetch Storj storage nodes and serve the local database over HTTP",
		RunE:  fetchCmd.RunE,
	}
	rootCmd.Flags().AddFlagSet(fetchCmd.Flags())
	rootCmd.PersistentFlags().AddFlagSet(fetchCmd.PersistentFlags())
	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(watchCmd)

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if runtimeCfg.dbPath == "" {
			runtimeCfg.dbPath = os.Getenv("STORJ_DB_PATH")
		}
		if runtimeCfg.dbPath == "" {
			runtimeCfg.dbPath = "nodes.db"
		}
		if rawEvery := os.Getenv("STORJ_UPDATE_EVERY"); runtimeCfg.updateEvery == 10*time.Second && rawEvery != "" {
			value, err := time.ParseDuration(rawEvery)
			if err != nil {
				return fmt.Errorf("parse STORJ_UPDATE_EVERY: %w", err)
			}
			runtimeCfg.updateEvery = value
		}
		runtimeCfg.printJSON = true
		return nil
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func formatWatchSummary(result fetchResult) string {
	if result.Summary == nil {
		return fmt.Sprintf("sat=%d fetched=%d", len(result.Satellites), result.Count)
	}

	return fmt.Sprintf(
		"sat=%d fetched=%d new_nodes=%d new_ips=%d changed=%d unchanged=%d db_nodes=%d db_ips=%d",
		result.Summary.SatellitesCount,
		result.Summary.FetchedNodes,
		result.Summary.NewNodes,
		result.Summary.NewIPs,
		result.Summary.ChangedNodes,
		result.Summary.UnchangedNodes,
		result.Summary.DatabaseTotalNodes,
		result.Summary.DatabaseTotalIPs,
	)
}

func executeFetch(ctx context.Context, cfg fetchConfig, satelliteConfigs []satelliteConfig, dbPath string) (fetchResult, error) {
	fetchedAtText := time.Now().UTC().Format(time.RFC3339)
	merged := make(map[string]node)
	satelliteResults := make([]satelliteFetchResult, 0, len(satelliteConfigs))

	for _, satCfg := range satelliteConfigs {
		satNodes, satelliteAddress, err := fetchNodes(ctx, cfg, satCfg)
		if err != nil {
			return fetchResult{}, err
		}
		satelliteResults = append(satelliteResults, satelliteFetchResult{
			Name:      satCfg.Name,
			Satellite: satelliteAddress,
			Count:     len(satNodes),
		})

		for _, n := range satNodes {
			existing, ok := merged[n.NodeID]
			if !ok {
				merged[n.NodeID] = n
				continue
			}
			existing.Satellites = mergeStrings(existing.Satellites, n.Satellites)
			if existing.Address == "" {
				existing.Address = n.Address
			}
			if existing.IP == "" {
				existing.IP = n.IP
			}
			if existing.Port == 0 {
				existing.Port = n.Port
			}
			merged[n.NodeID] = existing
		}
	}

	nodes := make([]node, 0, len(merged))
	for _, n := range merged {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })

	result := fetchResult{
		FetchedAt:  fetchedAtText,
		Count:      len(nodes),
		Nodes:      nodes,
		Satellites: satelliteResults,
	}
	if len(satelliteResults) == 1 {
		result.Satellite = satelliteResults[0].Satellite
	}

	store, err := openStorage(dbPath)
	if err != nil {
		return fetchResult{}, err
	}
	defer store.Close()

	fetchedAt, err := time.Parse(time.RFC3339, result.FetchedAt)
	if err != nil {
		return fetchResult{}, fmt.Errorf("parse fetched timestamp: %w", err)
	}
	summary, err := store.upsertNodes(nodes, cfg.bucket, cfg.objectKey, fetchedAt)
	if err != nil {
		return fetchResult{}, err
	}
	result.Summary = &fetchRunSummary{
		SatellitesCount:    len(satelliteResults),
		FetchedNodes:       len(nodes),
		NewNodes:           summary.NewNodes,
		NewIPs:             summary.NewIPs,
		UpdatedNodes:       summary.UpdatedNodes,
		ChangedNodes:       summary.ChangedNodes,
		UnchangedNodes:     summary.UnchangedNodes,
		DatabaseTotalNodes: summary.DatabaseTotalNodes,
		DatabaseTotalIPs:   summary.DatabaseTotalIPs,
	}

	return result, nil
}

func printFetchResult(result fetchResult, pretty bool) error {
	var (
		payload []byte
		err     error
	)
	if pretty {
		payload, err = json.MarshalIndent(result, "", "  ")
	} else {
		payload, err = json.Marshal(result)
	}
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}
	fmt.Println(string(payload))
	return nil
}

func loadDotEnv(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, found := strings.Cut(line, "=")
		if !found {
			return fmt.Errorf("invalid line in %s: %q", path, line)
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		value = strings.Trim(value, `"'`)
		if key == "" {
			return fmt.Errorf("empty key in %s", path)
		}

		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("set %s from %s: %w", key, path, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	return nil
}

func fetchNodes(ctx context.Context, cfg fetchConfig, satCfg satelliteConfig) ([]node, string, error) {
	apiKey, err := macaroon.ParseAPIKey(satCfg.APIKey)
	if err != nil {
		return nil, "", fmt.Errorf("parse api key: %w", err)
	}

	var proxyDialer proxy.ContextDialer
	if cfg.socks5 != "" {
		proxyDialer, err = parseSocksProxy(cfg.socks5)
		if err != nil {
			return nil, "", err
		}
	}

	client, satelliteAddress, err := dialMetainfo(ctx, satCfg.Satellite, apiKey, proxyDialer)
	if err != nil {
		return nil, "", err
	}
	defer client.Close()

	limits, err := requestAddressedOrderLimits(ctx, client, cfg.bucket, cfg.objectKey)
	if err != nil {
		return nil, "", err
	}

	nodes, err := normalizeNodes(limits)
	if err != nil {
		return nil, "", err
	}
	for i := range nodes {
		nodes[i].Satellites = []string{satelliteAddress}
	}
	return nodes, satelliteAddress, nil
}

func resolveSatelliteConfigs(cfg fetchConfig) ([]satelliteConfig, error) {
	if cfg.satellite != "" || cfg.apiKey != "" {
		if cfg.satellite == "" {
			return nil, errors.New("satellite is required")
		}
		if cfg.apiKey == "" {
			return nil, errors.New("api key is required via --api-key or STORJ_API_KEY")
		}
		return []satelliteConfig{{
			Name:      "cli",
			Satellite: cfg.satellite,
			APIKey:    cfg.apiKey,
		}}, nil
	}

	namesRaw := strings.TrimSpace(os.Getenv("STORJ_SATELLITE_CONFIGS"))
	if namesRaw != "" {
		var configs []satelliteConfig
		for _, item := range strings.Split(namesRaw, ",") {
			name := strings.ToUpper(strings.TrimSpace(item))
			if name == "" {
				continue
			}
			address := os.Getenv("STORJ_SATELLITE_" + name + "_ADDRESS")
			apiKey := os.Getenv("STORJ_SATELLITE_" + name + "_API_KEY")
			if address == "" || apiKey == "" {
				return nil, fmt.Errorf("missing address or api key for satellite config %s", name)
			}
			configs = append(configs, satelliteConfig{
				Name:      strings.ToLower(name),
				Satellite: address,
				APIKey:    apiKey,
			})
		}
		if len(configs) == 0 {
			return nil, errors.New("STORJ_SATELLITE_CONFIGS is set but no valid satellite configs were found")
		}
		return configs, nil
	}

	legacySatellite := os.Getenv("STORJ_SATELLITE_ADDRESS")
	legacyAPIKey := os.Getenv("STORJ_API_KEY")
	if legacySatellite == "" {
		return nil, errors.New("satellite is required")
	}
	if legacyAPIKey == "" {
		return nil, errors.New("api key is required via --api-key or STORJ_API_KEY")
	}
	return []satelliteConfig{{
		Name:      "default",
		Satellite: legacySatellite,
		APIKey:    legacyAPIKey,
	}}, nil
}

func mergeStrings(left, right []string) []string {
	seen := map[string]struct{}{}
	var merged []string
	for _, item := range append(append([]string{}, left...), right...) {
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		merged = append(merged, item)
	}
	sort.Strings(merged)
	return merged
}

func requestAddressedOrderLimits(ctx context.Context, client *metaclient.Client, bucket, objectKey string) ([]*pb.AddressedOrderLimit, error) {
	beginObjectReq := &metaclient.BeginObjectParams{
		Bucket:             []byte(bucket),
		EncryptedObjectKey: []byte(objectKey),
		ExpiresAt:          time.Now().Add(time.Minute),
	}

	beginSegmentReq := &metaclient.BeginSegmentParams{
		MaxOrderLimit: 67254016,
		Position: metaclient.SegmentPosition{
			Index: 0,
		},
	}

	responses, err := client.Batch(ctx, beginObjectReq, beginSegmentReq)
	if err != nil {
		return nil, fmt.Errorf("request segment limits: %w", err)
	}
	if len(responses) < 2 {
		return nil, fmt.Errorf("expected at least 2 batch responses, got %d", len(responses))
	}

	segmentResp, err := responses[1].BeginSegment()
	if err != nil {
		return nil, fmt.Errorf("decode begin segment response: %w", err)
	}
	return segmentResp.Limits, nil
}

func normalizeNodes(limits []*pb.AddressedOrderLimit) ([]node, error) {
	nodes := make([]node, 0, len(limits))
	seen := make(map[string]struct{}, len(limits))

	for _, limit := range limits {
		if limit == nil || limit.Limit == nil || limit.StorageNodeAddress == nil {
			continue
		}

		address := limit.StorageNodeAddress.Address
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("split host and port for %q: %w", address, err)
		}

		ip := strings.Trim(host, "[]")
		key := limit.Limit.StorageNodeId.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		portNum, err := net.LookupPort("tcp", port)
		if err != nil {
			return nil, fmt.Errorf("parse port for %q: %w", address, err)
		}

		nodes = append(nodes, node{
			NodeID:  key,
			Address: address,
			IP:      ip,
			Port:    portNum,
		})
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeID < nodes[j].NodeID
	})

	return nodes, nil
}

func dialMetainfo(ctx context.Context, satelliteAddress string, apiKey *macaroon.APIKey, proxyDialer proxy.ContextDialer) (*metaclient.Client, string, error) {
	ident, err := identity.NewFullIdentity(ctx, identity.NewCAOptions{
		Difficulty:  0,
		Concurrency: 1,
	})
	if err != nil {
		return nil, "", fmt.Errorf("create ephemeral identity: %w", err)
	}

	tlsOptions, err := tlsopts.NewOptions(ident, tlsopts.Config{
		UsePeerCAWhitelist: false,
		PeerIDVersions:     "0",
	}, nil)
	if err != nil {
		return nil, "", fmt.Errorf("configure tls options: %w", err)
	}

	dialer := rpc.NewDefaultDialer(tlsOptions)
	dialer.DialTimeout = 30 * time.Second
	if proxyDialer != nil {
		dialer.Connector = rpc.NewDefaultTCPConnector(proxyDialer.DialContext)
	}

	nodeURL, err := storj.ParseNodeURL(satelliteAddress)
	if err != nil {
		return nil, "", fmt.Errorf("parse satellite address: %w", err)
	}
	if nodeURL.ID.IsZero() {
		nodeID, found := rpc.KnownNodeID(nodeURL.Address)
		if !found {
			return nil, "", errors.New("satellite node ID is required for unknown satellites")
		}
		satelliteAddress = storj.NodeURL{
			ID:      nodeID,
			Address: nodeURL.Address,
		}.String()
	}

	client, err := metaclient.DialNodeURL(ctx, dialer, satelliteAddress, apiKey, "")
	if err != nil {
		return nil, "", fmt.Errorf("dial metainfo client: %w", err)
	}
	return client, satelliteAddress, nil
}

func parseSocksProxy(raw string) (proxy.ContextDialer, error) {
	parts := strings.SplitN(raw, ":", 4)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid socks5 proxy format %q", raw)
	}

	address := parts[0] + ":" + parts[1]
	var auth *proxy.Auth
	if len(parts) >= 3 {
		password := ""
		if len(parts) == 4 {
			password = parts[3]
		}
		auth = &proxy.Auth{User: parts[2], Password: password}
	}

	dialer, err := proxy.SOCKS5("tcp", address, auth, proxy.Direct)
	if err != nil {
		return nil, fmt.Errorf("create socks5 dialer: %w", err)
	}

	ctxDialer, ok := dialer.(proxy.ContextDialer)
	if !ok {
		return nil, errors.New("socks5 dialer does not implement ContextDialer")
	}
	return ctxDialer, nil
}
