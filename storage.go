package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type storage struct {
	db *sql.DB
}

type nodeRecord struct {
	NodeID      string `json:"node_id"`
	Address     string `json:"address"`
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Subnet24    string `json:"subnet_24"`
	Satellite   string `json:"satellite"`
	Bucket      string `json:"bucket"`
	ObjectKey   string `json:"object_key"`
	FirstSeenAt string `json:"first_seen_at"`
	LastSeenAt  string `json:"last_seen_at"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

type nodeHistoryRecord struct {
	ID          int64  `json:"id"`
	NodeID      string `json:"node_id"`
	Address     string `json:"address"`
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Subnet24    string `json:"subnet_24"`
	FirstSeenAt string `json:"first_seen_at"`
	LastSeenAt  string `json:"last_seen_at"`
	IsCurrent   bool   `json:"is_current"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

type statsWindow struct {
	NewNodes int `json:"new_nodes"`
	NewIPs   int `json:"new_ips"`
}

type statsSnapshot struct {
	GeneratedAt string                 `json:"generated_at"`
	TotalNodes  int                    `json:"total_nodes"`
	TotalIPs    int                    `json:"total_ips"`
	Windows     map[string]statsWindow `json:"windows"`
}

type upsertSummary struct {
	NewNodes           int
	NewIPs             int
	UpdatedNodes       int
	ChangedNodes       int
	UnchangedNodes     int
	DatabaseTotalNodes int
	DatabaseTotalIPs   int
}

func openStorage(path string) (*storage, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}

	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite database: %w", err)
	}

	store := &storage{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *storage) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *storage) init() error {
	const nodesSchema = `
CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    address TEXT NOT NULL,
    ip TEXT NOT NULL,
    port INTEGER NOT NULL,
    satellite TEXT NOT NULL,
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
`
	const historySchema = `
CREATE TABLE IF NOT EXISTS node_ip_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    address TEXT NOT NULL,
    ip TEXT NOT NULL,
    port INTEGER NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    is_current INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
`
	if _, err := s.db.Exec(nodesSchema); err != nil {
		return fmt.Errorf("initialize nodes table: %w", err)
	}
	if _, err := s.db.Exec(historySchema); err != nil {
		return fmt.Errorf("initialize node history table: %w", err)
	}

	if err := s.ensureColumn("nodes", "subnet_24", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := s.ensureColumn("node_ip_history", "subnet_24", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}

	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_nodes_ip ON nodes (ip);`,
		`CREATE INDEX IF NOT EXISTS idx_nodes_subnet24 ON nodes (subnet_24);`,
		`CREATE INDEX IF NOT EXISTS idx_nodes_address ON nodes (address);`,
		`CREATE INDEX IF NOT EXISTS idx_nodes_last_seen_at ON nodes (last_seen_at);`,
		`CREATE INDEX IF NOT EXISTS idx_node_ip_history_node_id ON node_ip_history (node_id);`,
		`CREATE INDEX IF NOT EXISTS idx_node_ip_history_subnet24 ON node_ip_history (subnet_24);`,
		`CREATE INDEX IF NOT EXISTS idx_node_ip_history_current ON node_ip_history (node_id, is_current);`,
	}
	for _, stmt := range indexes {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("initialize database indexes: %w", err)
		}
	}
	return nil
}

func (s *storage) ensureColumn(table, column, definition string) error {
	rows, err := s.db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return fmt.Errorf("inspect table %s: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk); err != nil {
			return fmt.Errorf("scan table info for %s: %w", table, err)
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read table info for %s: %w", table, err)
	}

	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, definition)
	if _, err := s.db.Exec(stmt); err != nil {
		return fmt.Errorf("add %s.%s: %w", table, column, err)
	}
	return nil
}

func (s *storage) upsertNodes(nodes []node, bucket, objectKey string, seenAt time.Time) (*upsertSummary, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin database transaction: %w", err)
	}

	fetchExistingStmt, err := tx.Prepare(`
SELECT ip, address, port
FROM nodes
WHERE node_id = ?
`)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("prepare select node statement: %w", err)
	}
	defer fetchExistingStmt.Close()

	fetchKnownIPStmt, err := tx.Prepare(`
SELECT 1
FROM node_ip_history
WHERE ip = ?
LIMIT 1
`)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("prepare select known ip statement: %w", err)
	}
	defer fetchKnownIPStmt.Close()

	upsertNodeStmt, err := tx.Prepare(`
INSERT INTO nodes (
    node_id, address, ip, port, subnet_24, satellite, bucket, object_key,
    first_seen_at, last_seen_at, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(node_id) DO UPDATE SET
    address = excluded.address,
    ip = excluded.ip,
    port = excluded.port,
    subnet_24 = excluded.subnet_24,
    satellite = excluded.satellite,
    bucket = excluded.bucket,
    object_key = excluded.object_key,
    last_seen_at = excluded.last_seen_at,
    updated_at = excluded.updated_at
`)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("prepare upsert node statement: %w", err)
	}
	defer upsertNodeStmt.Close()

	touchHistoryStmt, err := tx.Prepare(`
UPDATE node_ip_history
SET last_seen_at = ?, updated_at = ?
WHERE node_id = ? AND is_current = 1 AND ip = ? AND address = ? AND port = ?
`)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("prepare touch history statement: %w", err)
	}
	defer touchHistoryStmt.Close()

	closeHistoryStmt, err := tx.Prepare(`
UPDATE node_ip_history
SET last_seen_at = ?, updated_at = ?, is_current = 0
WHERE node_id = ? AND is_current = 1
`)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("prepare close history statement: %w", err)
	}
	defer closeHistoryStmt.Close()

	insertHistoryStmt, err := tx.Prepare(`
INSERT INTO node_ip_history (
    node_id, address, ip, port, subnet_24,
    first_seen_at, last_seen_at, is_current, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
`)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("prepare insert history statement: %w", err)
	}
	defer insertHistoryStmt.Close()

	stamp := seenAt.UTC().Format(time.RFC3339)
	summary := &upsertSummary{}
	for _, n := range nodes {
		satellite := strings.Join(n.Satellites, ",")
		subnet24, err := ipv4Subnet24(n.IP)
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("compute /24 for node %s: %w", n.NodeID, err)
		}

		var existingIP, existingAddress string
		var existingPort int
		selectErr := fetchExistingStmt.QueryRow(n.NodeID).Scan(&existingIP, &existingAddress, &existingPort)
		if selectErr != nil && !errors.Is(selectErr, sql.ErrNoRows) {
			_ = tx.Rollback()
			return nil, fmt.Errorf("read existing node %s: %w", n.NodeID, selectErr)
		}

		var ipMarker int
		ipWasKnown := false
		ipLookupErr := fetchKnownIPStmt.QueryRow(n.IP).Scan(&ipMarker)
		switch {
		case ipLookupErr == nil:
			ipWasKnown = true
		case errors.Is(ipLookupErr, sql.ErrNoRows):
		default:
			_ = tx.Rollback()
			return nil, fmt.Errorf("check existing ip %s for %s: %w", n.IP, n.NodeID, ipLookupErr)
		}

		_, err = upsertNodeStmt.Exec(
			n.NodeID,
			n.Address,
			n.IP,
			n.Port,
			subnet24,
			satellite,
			bucket,
			objectKey,
			stamp,
			stamp,
			stamp,
			stamp,
		)
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("upsert node %s: %w", n.NodeID, err)
		}

		switch {
		case errors.Is(selectErr, sql.ErrNoRows):
			summary.NewNodes++
			if _, err := insertHistoryStmt.Exec(n.NodeID, n.Address, n.IP, n.Port, subnet24, stamp, stamp, stamp, stamp); err != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("insert initial history for %s: %w", n.NodeID, err)
			}
			if !ipWasKnown {
				summary.NewIPs++
			}
		case existingIP == n.IP && existingAddress == n.Address && existingPort == n.Port:
			summary.UpdatedNodes++
			summary.UnchangedNodes++
			result, err := touchHistoryStmt.Exec(stamp, stamp, n.NodeID, n.IP, n.Address, n.Port)
			if err != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("touch history for %s: %w", n.NodeID, err)
			}
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("inspect history touch result for %s: %w", n.NodeID, err)
			}
			if rowsAffected == 0 {
				if _, err := insertHistoryStmt.Exec(n.NodeID, n.Address, n.IP, n.Port, subnet24, stamp, stamp, stamp, stamp); err != nil {
					_ = tx.Rollback()
					return nil, fmt.Errorf("backfill history for %s: %w", n.NodeID, err)
				}
			}
		default:
			summary.UpdatedNodes++
			summary.ChangedNodes++
			if _, err := closeHistoryStmt.Exec(stamp, stamp, n.NodeID); err != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("close current history for %s: %w", n.NodeID, err)
			}
			if _, err := insertHistoryStmt.Exec(n.NodeID, n.Address, n.IP, n.Port, subnet24, stamp, stamp, stamp, stamp); err != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("insert changed history for %s: %w", n.NodeID, err)
			}
			if !ipWasKnown {
				summary.NewIPs++
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit database transaction: %w", err)
	}

	summary.DatabaseTotalNodes, err = s.countScalar(`SELECT COUNT(*) FROM nodes`)
	if err != nil {
		return nil, fmt.Errorf("count total nodes after upsert: %w", err)
	}
	summary.DatabaseTotalIPs, err = s.countScalar(`SELECT COUNT(DISTINCT ip) FROM nodes WHERE ip <> ''`)
	if err != nil {
		return nil, fmt.Errorf("count total ips after upsert: %w", err)
	}

	return summary, nil
}

func (s *storage) getNode(nodeID string) (*nodeRecord, error) {
	row := s.db.QueryRow(`
SELECT node_id, address, ip, port, subnet_24, satellite, bucket, object_key,
       first_seen_at, last_seen_at, created_at, updated_at
FROM nodes
WHERE node_id = ?
`, nodeID)

	var rec nodeRecord
	if err := row.Scan(
		&rec.NodeID,
		&rec.Address,
		&rec.IP,
		&rec.Port,
		&rec.Subnet24,
		&rec.Satellite,
		&rec.Bucket,
		&rec.ObjectKey,
		&rec.FirstSeenAt,
		&rec.LastSeenAt,
		&rec.CreatedAt,
		&rec.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query node %s: %w", nodeID, err)
	}
	return &rec, nil
}

func (s *storage) getNodeHistory(nodeID string) ([]nodeHistoryRecord, error) {
	rows, err := s.db.Query(`
SELECT id, node_id, address, ip, port, subnet_24, first_seen_at, last_seen_at, is_current, created_at, updated_at
FROM node_ip_history
WHERE node_id = ?
ORDER BY id DESC
`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("query node history for %s: %w", nodeID, err)
	}
	defer rows.Close()

	var history []nodeHistoryRecord
	for rows.Next() {
		var rec nodeHistoryRecord
		var isCurrent int
		if err := rows.Scan(
			&rec.ID,
			&rec.NodeID,
			&rec.Address,
			&rec.IP,
			&rec.Port,
			&rec.Subnet24,
			&rec.FirstSeenAt,
			&rec.LastSeenAt,
			&isCurrent,
			&rec.CreatedAt,
			&rec.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan node history for %s: %w", nodeID, err)
		}
		rec.IsCurrent = isCurrent == 1
		history = append(history, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read node history for %s: %w", nodeID, err)
	}
	return history, nil
}

func (s *storage) getNodesByIP(ip string) ([]nodeRecord, error) {
	rows, err := s.db.Query(`
SELECT node_id, address, ip, port, subnet_24, satellite, bucket, object_key,
       first_seen_at, last_seen_at, created_at, updated_at
FROM nodes
WHERE ip = ?
ORDER BY node_id
`, ip)
	if err != nil {
		return nil, fmt.Errorf("query nodes by ip %s: %w", ip, err)
	}
	defer rows.Close()
	return scanNodeRecords(rows)
}

func (s *storage) getNodesBySubnet(subnet24 string) ([]nodeRecord, error) {
	rows, err := s.db.Query(`
SELECT node_id, address, ip, port, subnet_24, satellite, bucket, object_key,
       first_seen_at, last_seen_at, created_at, updated_at
FROM nodes
WHERE subnet_24 = ?
ORDER BY node_id
`, subnet24)
	if err != nil {
		return nil, fmt.Errorf("query nodes by subnet %s: %w", subnet24, err)
	}
	defer rows.Close()
	return scanNodeRecords(rows)
}

func (s *storage) getStats(now time.Time) (*statsSnapshot, error) {
	totalNodes, err := s.countScalar(`SELECT COUNT(*) FROM nodes`)
	if err != nil {
		return nil, fmt.Errorf("count total nodes: %w", err)
	}
	totalIPs, err := s.countScalar(`SELECT COUNT(DISTINCT ip) FROM nodes WHERE ip <> ''`)
	if err != nil {
		return nil, fmt.Errorf("count total ips: %w", err)
	}

	windows := map[string]time.Duration{
		"last_hour":     time.Hour,
		"last_24_hours": 24 * time.Hour,
		"last_week":     7 * 24 * time.Hour,
	}

	snapshot := &statsSnapshot{
		GeneratedAt: now.UTC().Format(time.RFC3339),
		TotalNodes:  totalNodes,
		TotalIPs:    totalIPs,
		Windows:     make(map[string]statsWindow, len(windows)),
	}

	for name, duration := range windows {
		cutoff := now.UTC().Add(-duration).Format(time.RFC3339)
		newNodes, err := s.countScalar(`SELECT COUNT(*) FROM nodes WHERE first_seen_at >= ?`, cutoff)
		if err != nil {
			return nil, fmt.Errorf("count new nodes for %s: %w", name, err)
		}
		newIPs, err := s.countScalar(`
SELECT COUNT(*)
FROM (
    SELECT ip
    FROM node_ip_history
    WHERE ip <> ''
    GROUP BY ip
    HAVING MIN(first_seen_at) >= ?
)`, cutoff)
		if err != nil {
			return nil, fmt.Errorf("count new ips for %s: %w", name, err)
		}
		snapshot.Windows[name] = statsWindow{
			NewNodes: newNodes,
			NewIPs:   newIPs,
		}
	}

	return snapshot, nil
}

func (s *storage) countScalar(query string, args ...any) (int, error) {
	var value int
	if err := s.db.QueryRow(query, args...).Scan(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func scanNodeRecords(rows *sql.Rows) ([]nodeRecord, error) {
	var nodes []nodeRecord
	for rows.Next() {
		var rec nodeRecord
		if err := rows.Scan(
			&rec.NodeID,
			&rec.Address,
			&rec.IP,
			&rec.Port,
			&rec.Subnet24,
			&rec.Satellite,
			&rec.Bucket,
			&rec.ObjectKey,
			&rec.FirstSeenAt,
			&rec.LastSeenAt,
			&rec.CreatedAt,
			&rec.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan node row: %w", err)
		}
		nodes = append(nodes, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read node rows: %w", err)
	}
	return nodes, nil
}

func ipv4Subnet24(ip string) (string, error) {
	parsed := net.ParseIP(strings.TrimSpace(ip))
	if parsed == nil {
		return "", fmt.Errorf("invalid ip %q", ip)
	}
	v4 := parsed.To4()
	if v4 == nil {
		return "", fmt.Errorf("ip %q is not IPv4", ip)
	}
	return fmt.Sprintf("%d.%d.%d.0/24", v4[0], v4[1], v4[2]), nil
}

func normalizeSubnet24(input string) (string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", errors.New("subnet is required")
	}
	if strings.Contains(input, "/") {
		ip, network, err := net.ParseCIDR(input)
		if err != nil {
			return "", fmt.Errorf("parse cidr %q: %w", input, err)
		}
		ones, bits := network.Mask.Size()
		if bits != 32 {
			return "", fmt.Errorf("subnet %q is not IPv4", input)
		}
		if ones != 24 {
			return "", fmt.Errorf("subnet %q must be /24", input)
		}
		return ipv4Subnet24(ip.String())
	}
	return ipv4Subnet24(input)
}
