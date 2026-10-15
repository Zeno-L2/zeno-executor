package wasm

import (
	"testing"

	"github.com/zenon-network/go-zenon/vm/constants"

	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

func TestModuleLimitsMapping(t *testing.T) {
	want := module.Limits{
		MaxCodeSize:            MaxCodeSize,
		MaxMemoryPages:         MaxMemoryPages,
		MaxLocalsPerFunction:   MaxLocalsPerFunction,
		MaxTotalLocals:         MaxTotalLocals,
		MaxTableEntries:        MaxTableEntries,
		MaxTotalParams:         MaxTotalParams,
		MaxGlobals:             MaxGlobals,
		MaxDataSegments:        MaxDataSegments,
		MaxFunctions:           MaxFunctions,
		MaxTotalElementEntries: MaxTotalElementEntries,
		MaxTypeCount:           MaxTypeCount,
	}
	got := moduleLimits()
	if got != want {
		t.Fatalf("moduleLimits() = %+v, want %+v", got, want)
	}
}

func TestMaxExecutionResultMatchesSettlement(t *testing.T) {
	if MaxExecutionResult != constants.MaxExecutionResult {
		t.Fatalf("MaxExecutionResult = %d, Settlement's constants.MaxExecutionResult = %d", MaxExecutionResult, constants.MaxExecutionResult)
	}
}
