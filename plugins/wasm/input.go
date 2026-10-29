package wasm

import (
	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
)

// Input is the WASM domain's typed, decoded input: the runtime-observed
// context plus either a decoded codec.InputArgs tail (a kind this
// domain implements) or Unsupported, set for any input_kind outside
// {0..7} so Apply can emit UNSUPPORTED_OPERATION rather than halt.
// There is no wire format here: Ctx crosses the seam as a plain Go
// struct, never bytes.
type Input struct {
	Ctx         domain.InputContext
	Args        codec.InputArgs
	Unsupported bool
}
