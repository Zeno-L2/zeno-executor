package engine

import (
	"errors"
	"fmt"
)

// OutcomeKind is the deterministic classification Run assigns to one call. 04f maps it onto
// the §15 receipt status; this package owns no status vocabulary.
type OutcomeKind uint8

const (
	Returned OutcomeKind = iota
	Aborted
	OutOfGas
	Trapped
)

// Outcome is Run's deterministic result for one input. Every field is meaningful only for the
// Kind it is documented against; the rest hold their zero value.
type Outcome struct {
	Kind OutcomeKind
	// Ret is meaningful for Returned only; 0 otherwise. For Ret >= 0 it is the packed
	// (result_ptr << 32) | result_len.
	Ret       int64
	AbortCode int32 // Aborted only; 0 otherwise
	GasUsed   uint64
	// ResultBlob is populated for Returned with Ret >= 0 only: a copy of exactly result_len
	// bytes at result_ptr, never a view into module memory and never truncated at any length.
	// It is a copy because it outlives the instance: the module instance, and its memory with
	// it, is torn down before Run returns.
	ResultBlob []byte
}

// abortError is the typed abort a contract raises by calling env.abort(code); classify maps it
// to Aborted regardless of the ledger's state.
type abortError struct{ code int32 }

func (e *abortError) Error() string { return fmt.Sprintf("engine: abort(%d)", e.code) }

// internalFault is a node-local fault raised inside a host function body: a domain.StateAccess
// error, or a failed ledger accessor call. It is not a consensus outcome: Run unwraps it and
// returns it through its error channel, which the seam defines as halt. Classifying a
// node-local I/O error as a receipt status would let two honest nodes commit different
// post-states.
type internalFault struct{ err error }

func (f *internalFault) Error() string { return fmt.Sprintf("engine: internal fault: %v", f.err) }
func (f *internalFault) Unwrap() error { return f.err }

// errHostOutOfGas is the sentinel a host charge panics with after writing -1 to the ledger. It
// is not consulted by classify (the ledger is); it only makes the trap legible.
var errHostOutOfGas = fmt.Errorf("engine: out of gas in host function")

// classify implements the outcome priority rule in strict order, so a deterministic outcome is
// never reclassified by a coincident condition:
//
//  1. callErr unwraps to *abortError => Aborted;
//  2. callErr != nil && gas < 0 => OutOfGas;
//  3. callErr != nil => Trapped;
//  4. callErr == nil => Returned.
//
// Callers must check for *internalFault and return early before calling classify: a halt is not
// a receipt outcome at all.
func classify(callErr error, gas int64) OutcomeKind {
	var abortErr *abortError
	if errors.As(callErr, &abortErr) {
		return Aborted
	}
	if callErr != nil && gas < 0 {
		return OutOfGas
	}
	if callErr != nil {
		return Trapped
	}
	return Returned
}
