package wasm

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
)

// failsCheck runs check on its own goroutine with a throwaway
// testing.T and reports whether it called Fatalf.
func failsCheck(check func(t *testing.T)) bool {
	ft := &testing.T{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		check(ft)
	}()
	<-done
	return ft.Failed()
}

func loadUndeployedRows(t *testing.T) map[string]vectorJSON {
	t.Helper()
	data, err := os.ReadFile(vectorFilePath)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", vectorFilePath, err)
	}
	var file vectorFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	rows := map[string]vectorJSON{}
	for _, v := range file.Vectors {
		if v.Shape == "undeployed_target" {
			rows[v.ID] = v
		}
	}
	for _, id := range []string{"CV-DEPOSIT-UNDEPLOYED-1", "CV-CALL-UNDEPLOYED-1"} {
		if _, ok := rows[id]; !ok {
			t.Fatalf("%s is not an undeployed_target row", id)
		}
	}
	if len(rows) != 2 {
		t.Fatalf("undeployed_target rows = %d, want 2", len(rows))
	}
	return rows
}

// withResult returns vec with its execution result blob rewritten by edit.
func withResult(t *testing.T, vec vectorJSON, edit func(*codec.ExecutionResult)) vectorJSON {
	t.Helper()
	er, err := codec.DecodeExecutionResult(mustHex(t, vec.ExecutionResultBlobHex))
	if err != nil {
		t.Fatalf("DecodeExecutionResult: %v", err)
	}
	edit(&er)
	blob, err := er.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	vec.ExecutionResultBlobHex = hex.EncodeToString(blob)
	return vec
}

// TestUndeployedTargetCheckRejectsMutations proves checkUndeployedTarget
// refuses a row whose status, gas, refund, witness set, or module/frame
// emptiness departs from the undeployed-target outcome.
func TestUndeployedTargetCheckRejectsMutations(t *testing.T) {
	rows := loadUndeployedRows(t)

	mutations := map[string]func(*testing.T, vectorJSON) vectorJSON{
		"status":             func(_ *testing.T, v vectorJSON) vectorJSON { v.Status = "REVERT"; return v },
		"gas_used":           func(_ *testing.T, v vectorJSON) vectorJSON { v.GasUsed = 7; return v },
		"receipt abort_code": func(_ *testing.T, v vectorJSON) vectorJSON { v.Receipt.AbortCode = 1; return v },
		"receipt code_hash":  func(_ *testing.T, v vectorJSON) vectorJSON { v.Receipt.CodeHash = v.Receipt.ContractID; return v },
		"refund dropped": func(t *testing.T, v vectorJSON) vectorJSON {
			return withResult(t, v, func(er *codec.ExecutionResult) { er.Outbox = nil })
		},
		"refund partial": func(t *testing.T, v vectorJSON) vectorJSON {
			return withResult(t, v, func(er *codec.ExecutionResult) {
				w := er.Outbox[0].Target.(codec.L1Withdrawal)
				w.Amount[31]--
				er.Outbox[0].Target = w
			})
		},
		"witness one leaf": func(_ *testing.T, v vectorJSON) vectorJSON {
			v.WitnessKeys = append([]string(nil), v.WitnessKeys[:1]...)
			return v
		},
		"witness extra leaf": func(_ *testing.T, v vectorJSON) vectorJSON {
			v.WitnessKeys = append(append([]string(nil), v.WitnessKeys...), zeroHashHex)
			return v
		},
		"witness reordered": func(_ *testing.T, v vectorJSON) vectorJSON {
			v.WitnessKeys = []string{v.WitnessKeys[1], v.WitnessKeys[0]}
			return v
		},
		"pre_state holds a leaf": func(_ *testing.T, v vectorJSON) vectorJSON {
			v.PreState = map[string]string{v.WitnessKeys[0]: zeroHashHex}
			return v
		},
		"module present":    func(_ *testing.T, v vectorJSON) vectorJSON { v.ModuleWasmHex = "0061736d"; return v },
		"frame present":     func(_ *testing.T, v vectorJSON) vectorJSON { v.InputFrameHex = "00"; return v },
		"code_hash present": func(_ *testing.T, v vectorJSON) vectorJSON { v.CodeHash = v.Receipt.ContractID; return v },
		"instrumented hash": func(_ *testing.T, v vectorJSON) vectorJSON { v.InstrumentedCodeHash = v.Receipt.ContractID; return v },
		"effects blob":      func(_ *testing.T, v vectorJSON) vectorJSON { v.ContractEffectsBlobHex = "00"; return v },
		"pending metadata":  func(_ *testing.T, v vectorJSON) vectorJSON { s := ""; v.PendingMetadataHex = &s; return v },
		"unsupported kind":  func(_ *testing.T, v vectorJSON) vectorJSON { v.InputKind = 4; return v },
		"zero deposit":      func(_ *testing.T, v vectorJSON) vectorJSON { v.Input.DepositAmount = zeroHashHex; return v },
		"receipt contract":  func(_ *testing.T, v vectorJSON) vectorJSON { v.Receipt.ContractID = zeroHashHex; return v },
	}

	for id, row := range rows {
		id, row := id, row
		if failsCheck(func(ft *testing.T) { checkUndeployedTarget(ft, row) }) {
			t.Fatalf("%s: checkUndeployedTarget rejects the unmutated row", id)
		}
		for name, mutate := range mutations {
			name, mutate := name, mutate
			t.Run(id+"/"+name, func(t *testing.T) {
				in := row
				inp := *row.Input
				in.Input = &inp
				in.Receipt = row.Receipt
				mutated := mutate(t, in)
				if !failsCheck(func(ft *testing.T) { checkUndeployedTarget(ft, mutated) }) {
					t.Fatalf("checkUndeployedTarget accepts the %s mutation", name)
				}
			})
		}
	}
}
