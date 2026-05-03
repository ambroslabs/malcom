package snapshotdiff

import (
	"bytes"
	"crypto/sha256"
	"fmt"
)

// VerifyResult summarises the diff between two snapshot directories
// compared by walking their stores in lockstep.
type VerifyResult struct {
	Stores        int
	ItemsCompared uint64
	ExtraInA      uint64
	ExtraInB      uint64
	ValueDiffs    uint64
	ExtensionDiff int
	PerStore      map[string]VerifyStoreStats
}

// VerifyStoreStats is the per-store breakdown of differences.
type VerifyStoreStats struct {
	ItemsA     uint64
	ItemsB     uint64
	ExtraInA   uint64
	ExtraInB   uint64
	ValueDiffs uint64
}

// VerifyEquivalent walks two snapshots and reports per-store and per-
// extension differences in their logical content. Byte-level snapshot
// differences (chunk boundaries, IAVL version metadata) are ignored —
// only (store, key, value) triples and (extension, payload-set) are
// compared.
func VerifyEquivalent(dirA, dirB string) (VerifyResult, error) {
	var res VerifyResult
	res.PerStore = map[string]VerifyStoreStats{}

	rA, err := newStoreReader(dirA)
	if err != nil {
		return res, fmt.Errorf("open A: %w", err)
	}
	defer rA.Close()
	rB, err := newStoreReader(dirB)
	if err != nil {
		return res, fmt.Errorf("open B: %w", err)
	}
	defer rB.Close()

	for {
		sA, dA, err := rA.peekStore()
		if err != nil {
			return res, err
		}
		sB, dB, err := rB.peekStore()
		if err != nil {
			return res, err
		}
		if dA && dB {
			break
		}
		var name string
		var inA, inB bool
		switch {
		case dA:
			name, inB = sB, true
		case dB:
			name, inA = sA, true
		case sA == sB:
			name, inA, inB = sA, true, true
		case sA < sB:
			name, inA = sA, true
		default:
			name, inB = sB, true
		}

		var ss VerifyStoreStats
		if err := compareStore(rA, rB, name, inA, inB, &ss); err != nil {
			return res, err
		}
		res.Stores++
		res.PerStore[name] = ss
		res.ExtraInA += ss.ExtraInA
		res.ExtraInB += ss.ExtraInB
		res.ValueDiffs += ss.ValueDiffs
		res.ItemsCompared += ss.ItemsA + ss.ItemsB
	}

	extA, err := rA.readAllExtensions()
	if err != nil {
		return res, err
	}
	extB, err := rB.readAllExtensions()
	if err != nil {
		return res, err
	}
	res.ExtensionDiff = compareExtensions(extA, extB)

	return res, nil
}

// compareStore advances rA and rB through one store, counting per-key
// differences. inA / inB indicate whether each side actually has this
// store (the alphabetical merge in VerifyEquivalent decides that).
func compareStore(rA, rB *storeReader, name string, inA, inB bool, ss *VerifyStoreStats) error {
	var keyA, valA, keyB, valB []byte
	hasA, hasB := false, false

	advA := func() error {
		if !inA {
			return nil
		}
		hasA = false
		for !hasA {
			done, err := rA.readNextItemInStoreRaw(name, func(rec []byte) {
				if !isLeafIAVL(rec) {
					return
				}
				k := parseProtoBytes(rec, 1)
				v := parseProtoBytes(rec, 2)
				keyA = append(keyA[:0], k...)
				valA = append(valA[:0], v...)
				hasA = true
				ss.ItemsA++
			})
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
		return nil
	}
	advB := func() error {
		if !inB {
			return nil
		}
		hasB = false
		for !hasB {
			done, err := rB.readNextItemInStoreRaw(name, func(rec []byte) {
				if !isLeafIAVL(rec) {
					return
				}
				k := parseProtoBytes(rec, 1)
				v := parseProtoBytes(rec, 2)
				keyB = append(keyB[:0], k...)
				valB = append(valB[:0], v...)
				hasB = true
				ss.ItemsB++
			})
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
		return nil
	}

	if err := advA(); err != nil {
		return err
	}
	if err := advB(); err != nil {
		return err
	}

	for hasA || hasB {
		switch {
		case hasA && !hasB:
			ss.ExtraInA++
			if err := advA(); err != nil {
				return err
			}
		case hasB && !hasA:
			ss.ExtraInB++
			if err := advB(); err != nil {
				return err
			}
		default:
			cmp := bytes.Compare(keyA, keyB)
			switch {
			case cmp < 0:
				ss.ExtraInA++
				if err := advA(); err != nil {
					return err
				}
			case cmp > 0:
				ss.ExtraInB++
				if err := advB(); err != nil {
					return err
				}
			default:
				if !bytes.Equal(valA, valB) {
					ss.ValueDiffs++
				}
				if err := advA(); err != nil {
					return err
				}
				if err := advB(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// compareExtensions reports the number of payloads that differ between A
// and B by sha256 (extensions are unordered payload sets).
func compareExtensions(a, b map[string]*extData) int {
	diffs := 0
	names := map[string]bool{}
	for n := range a {
		names[n] = true
	}
	for n := range b {
		names[n] = true
	}
	for name := range names {
		ea := a[name]
		eb := b[name]
		setA := map[[32]byte]bool{}
		setB := map[[32]byte]bool{}
		if ea != nil {
			for _, p := range ea.payloads {
				setA[sha256.Sum256(p)] = true
			}
		}
		if eb != nil {
			for _, p := range eb.payloads {
				setB[sha256.Sum256(p)] = true
			}
		}
		for h := range setA {
			if !setB[h] {
				diffs++
			}
		}
		for h := range setB {
			if !setA[h] {
				diffs++
			}
		}
	}
	return diffs
}
