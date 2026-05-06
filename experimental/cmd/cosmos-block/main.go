// cosmos-block reads one block from the archive and prints it.
//
// Default output is a JSON dump (header + data + commit + ProposerAddress)
// matching what cmd/cosmos-p2p produced. Use -raw to write the
// proto-marshaled bytes to stdout for piping into other tools.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/types"
	"github.com/cosmos/gogoproto/proto"

	"github.com/zrbecker/cosmos-p2p/internal/archive"
)

func main() {
	var (
		dir    = flag.String("archive", "/mnt/data/cosmos-archive/cosmoshub-4", "archive root directory")
		height = flag.Int64("height", 0, "block height to read")
		raw    = flag.Bool("raw", false, "write raw cmtproto.Block bytes to stdout instead of JSON")
		summary = flag.Bool("summary", false, "print one-line summary instead of full JSON")
	)
	flag.Parse()

	if *height <= 0 {
		fmt.Fprintln(os.Stderr, "error: -height required (>0)")
		os.Exit(2)
	}

	st, err := archive.New(*dir)
	if err != nil {
		log.Fatalf("open archive: %v", err)
	}
	defer st.Close()

	bytes, err := st.Get(uint64(*height))
	if err != nil {
		log.Fatalf("read block %d: %v", *height, err)
	}

	if *raw {
		if _, err := os.Stdout.Write(bytes); err != nil {
			log.Fatal(err)
		}
		return
	}

	var pb cmtproto.Block
	if err := proto.Unmarshal(bytes, &pb); err != nil {
		log.Fatalf("decode block: %v", err)
	}
	block, err := types.BlockFromProto(&pb)
	if err != nil {
		log.Fatalf("BlockFromProto: %v", err)
	}

	if *summary {
		fmt.Printf("height=%d  hash=%X  time=%s  txs=%d  proposer=%X  bytes=%d\n",
			block.Height,
			block.Hash(),
			block.Time.UTC().Format(time.RFC3339),
			len(block.Txs),
			block.ProposerAddress,
			len(bytes),
		)
		return
	}

	type out struct {
		Height      int64        `json:"height"`
		Hash        string       `json:"hash"`
		Time        string       `json:"time"`
		ChainID     string       `json:"chain_id"`
		Proposer    string       `json:"proposer_address"`
		NumTxs      int          `json:"num_txs"`
		TxHashes    []string     `json:"tx_hashes"`
		StoredBytes int          `json:"stored_bytes"`
		Block       *types.Block `json:"block"`
	}
	hashes := make([]string, len(block.Txs))
	for i, tx := range block.Txs {
		hashes[i] = strings.ToUpper(fmt.Sprintf("%X", tx.Hash()))
	}
	o := out{
		Height:      block.Height,
		Hash:        strings.ToUpper(fmt.Sprintf("%X", block.Hash())),
		Time:        block.Time.UTC().Format(time.RFC3339Nano),
		ChainID:     block.ChainID,
		Proposer:    strings.ToUpper(fmt.Sprintf("%X", block.ProposerAddress)),
		NumTxs:      len(block.Txs),
		TxHashes:    hashes,
		StoredBytes: len(bytes),
		Block:       block,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(o); err != nil {
		log.Fatal(err)
	}
}
