// Package verify is the `malcom verify` subcommand: read a CommitInfo
// from an application.db produced by `malcom snapshot import`, compute
// the cosmos-sdk MultiStore AppHash, fetch the consensus AppHash from a
// cometbft RPC, and compare.
//
// Pebble-only — the goleveldb path was dropped along with the
// internal/snapshotappdb importer that depended on github.com/cosmos/iavl.
package verify

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/cometbft/cometbft/crypto/merkle"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
)

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom verify", flag.ContinueOnError)
	chain := fs.String("chain", "", "chain id (required)")
	appdb := fs.String("appdb", "", "path to the application.db parent dir (required)")
	height := fs.Int64("height", 0, "snapshot height committed to application.db (required)")
	rpcURL := fs.String("rpc", "", "cometbft RPC endpoint (defaults to first chains/<id>.toml rpcs entry)")
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	debug := fs.Bool("debug", false, "verbose logging")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *appdb == "" || *height == 0 || *chain == "" {
		fs.Usage()
		return 2
	}

	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return 2
	}
	// Pull [log] from config when available; verify is also runnable
	// with -rpc and no config, in which case fall back to defaults.
	var logTuning malcomlog.Tuning
	if cfg, err := config.Load(); err == nil {
		if ch, err := cfg.Resolve(*chain); err == nil {
			logTuning = malcomlog.Tuning{Level: ch.Log.Level, Modules: ch.Log.Modules}
		} else {
			logTuning = malcomlog.Tuning{Level: cfg.Log.Level, Modules: cfg.Log.Modules}
		}
	}
	logOpts, err := malcomlog.BuildOptions(logTuning, mode, *debug, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log config: %v\n", err)
		return 2
	}
	log := malcomlog.New(logOpts).With("module", "verify")

	// -rpc lets verify run against a single RPC without needing a
	// chains/<id>.toml — convenient for one-off checks. Only load
	// config if we need it (no explicit -rpc).
	rpc := *rpcURL
	var ch config.Chain
	if rpc == "" {
		cfg, err := config.Load()
		if err != nil {
			log.Error("config load", "err", err)
			return 1
		}
		ch, err = cfg.Resolve(*chain)
		if err != nil {
			log.Error("resolve chain", "err", err, "chain", *chain)
			return 1
		}
	} else {
		ch.ChainID = *chain
	}
	if rpc == "" && len(ch.RPCs) > 0 {
		rpc = ch.RPCs[0]
	}
	if rpc == "" {
		log.Error("no rpc; pass -rpc or set chains.<id>.rpcs in config", "chain", ch.ChainID)
		return 1
	}

	log.Info("starting", "appdb", *appdb, "height", *height, "rpc", rpc)

	infos, err := readCommitInfo(*appdb, *height)
	if err != nil {
		log.Error("read commit info failed", "err", err)
		return 1
	}
	log.Info("commit info read", "stores", len(infos))
	for _, si := range infos {
		log.Debug("store", "name", si.Name, "hash", fmt.Sprintf("%x", si.Hash))
	}

	localHash := computeAppHash(infos)
	log.Info("local apphash", "apphash", fmt.Sprintf("%X", localHash))

	consHeight := *height + 1
	consHash, err := fetchAppHash(rpc, consHeight)
	if err != nil {
		log.Error("fetch consensus apphash failed", "err", err, "rpc", rpc, "height", consHeight)
		return 1
	}
	log.Info("consensus apphash", "height", consHeight, "apphash", consHash)

	consBytes, _ := hex.DecodeString(consHash)
	if bytes.Equal(localHash, consBytes) {
		log.Info("MATCH — application.db is consensus-correct", "height", *height)
		return 0
	}
	log.Error("MISMATCH",
		"local", fmt.Sprintf("%X", localHash),
		"consensus", consHash)
	return 1
}

type storeInfo struct {
	Name string
	Hash []byte
}

func readCommitInfo(appdbParent string, height int64) ([]storeInfo, error) {
	dbPath := filepath.Join(appdbParent, "application.db")
	db, err := pebble.Open(dbPath, &pebble.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()

	key := []byte(fmt.Sprintf("s/%d", height))
	val, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, fmt.Errorf("no CommitInfo at key %q", key)
	}
	if err != nil {
		return nil, err
	}
	buf := make([]byte, len(val))
	copy(buf, val)
	closer.Close()
	return parseCommitInfo(buf)
}

// parseCommitInfo decodes the CommitInfo proto we wrote in
// internal/snapshotimport/commitinfo.go: commitInfoBytes.
//
//	message CommitID    { int64 version = 1; bytes hash = 2; }
//	message StoreInfo   { string name = 1; CommitID commit_id = 2; }
//	message CommitInfo  { int64 version = 1; repeated StoreInfo store_infos = 2; }
func parseCommitInfo(buf []byte) ([]storeInfo, error) {
	var infos []storeInfo
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		field := int(tag >> 3)
		wire := tag & 0x07
		switch {
		case field == 1 && wire == 0:
			_, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return nil, fmt.Errorf("bad version")
			}
			i += m
		case field == 2 && wire == 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return nil, fmt.Errorf("bad store_info length")
			}
			i += m
			si, err := parseStoreInfo(buf[i : i+int(ln)])
			if err != nil {
				return nil, err
			}
			infos = append(infos, si)
			i += int(ln)
		default:
			return nil, fmt.Errorf("unexpected field %d wire %d in CommitInfo", field, wire)
		}
	}
	return infos, nil
}

func parseStoreInfo(buf []byte) (storeInfo, error) {
	var si storeInfo
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		field := int(tag >> 3)
		wire := tag & 0x07
		switch {
		case field == 1 && wire == 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return si, fmt.Errorf("bad name length")
			}
			i += m
			si.Name = string(buf[i : i+int(ln)])
			i += int(ln)
		case field == 2 && wire == 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return si, fmt.Errorf("bad commit_id length")
			}
			i += m
			h, err := parseCommitID(buf[i : i+int(ln)])
			if err != nil {
				return si, err
			}
			si.Hash = h
			i += int(ln)
		default:
			return si, fmt.Errorf("unexpected field %d wire %d in StoreInfo", field, wire)
		}
	}
	return si, nil
}

func parseCommitID(buf []byte) ([]byte, error) {
	var hash []byte
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		field := int(tag >> 3)
		wire := tag & 0x07
		switch {
		case field == 1 && wire == 0:
			_, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return nil, fmt.Errorf("bad commitID version")
			}
			i += m
		case field == 2 && wire == 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return nil, fmt.Errorf("bad commitID hash length")
			}
			i += m
			hash = append([]byte(nil), buf[i:i+int(ln)]...)
			i += int(ln)
		default:
			return nil, fmt.Errorf("unexpected field %d wire %d in CommitID", field, wire)
		}
	}
	return hash, nil
}

func fetchAppHash(rpcBase string, height int64) (string, error) {
	url := fmt.Sprintf("%s/commit?height=%d", strings.TrimRight(rpcBase, "/"), height)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("rpc HTTP %d: %s", resp.StatusCode, body)
	}
	var raw map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return "", err
	}
	result, _ := raw["result"].(map[string]interface{})
	signed, _ := result["signed_header"].(map[string]interface{})
	header, _ := signed["header"].(map[string]interface{})
	apphash, _ := header["app_hash"].(string)
	if apphash == "" {
		return "", fmt.Errorf("no app_hash in response")
	}
	return apphash, nil
}

// computeAppHash reproduces cosmos-sdk's CommitInfo.Hash() — the
// MultiStore AppHash that gaiad would compute and ICS-23-prove against.
//
//  1. For each (store_name, store_root_hash):
//     leaf_input = uvarint(len(name)) || name
//                  || uvarint(32)     || sha256(store_root_hash)
//
//     The sha256 of the value is the cosmos-sdk quirk: simpleMap.Set
//     pre-hashes values before adding to the map. Load-bearing for the
//     AppHash output even though it's not load-bearing for consensus.
//
//  2. Sort leaves lexicographically by store name.
//
//  3. Run cometbft's RFC6962 simple-merkle (HashFromByteSlices) over
//     the sorted leaf inputs. cometbft applies the leaf-prefix (0x00)
//     and inner-prefix (0x01) internally.
func computeAppHash(infos []storeInfo) []byte {
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	items := make([][]byte, len(infos))
	for i, si := range infos {
		vhash := sha256.Sum256(si.Hash)
		var buf bytes.Buffer
		writeUvarint(&buf, uint64(len(si.Name)))
		buf.WriteString(si.Name)
		writeUvarint(&buf, uint64(len(vhash)))
		buf.Write(vhash[:])
		items[i] = buf.Bytes()
	}
	return merkle.HashFromByteSlices(items)
}

func writeUvarint(w io.Writer, v uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	w.Write(buf[:n])
}
