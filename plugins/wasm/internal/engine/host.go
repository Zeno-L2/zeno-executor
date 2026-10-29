package engine

import (
	"context"
	"fmt"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/keys"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/module"
)

// HostContext is the per-input state the env host module reads. One Engine serves every
// input; the context.Context value is the only per-input channel.
type HostContext struct {
	State      domain.StateAccess
	ContractID types.Hash
}

type hostContextKey struct{}

func withHostContext(ctx context.Context, hc HostContext) context.Context {
	return context.WithValue(ctx, hostContextKey{}, hc)
}

// hostContext returns the context Run installed. The type assertion is unchecked: Run is the
// only path that reaches a host function and it always installs one.
func hostContext(ctx context.Context) HostContext {
	return ctx.Value(hostContextKey{}).(HostContext)
}

// instantiateHostModule builds and instantiates the env module once per Engine, with
// WithGoModuleFunction for all three functions: the raw-stack form makes the exact
// api.ValueType signature explicit, and it must match validate.go's import signatures byte for
// byte.
func instantiateHostModule(ctx context.Context, r wazero.Runtime) (api.Module, error) {
	i32 := api.ValueTypeI32
	i64 := api.ValueTypeI64

	return r.NewHostModuleBuilder("env").
		NewFunctionBuilder().
		WithGoModuleFunction(api.GoModuleFunc(storageLen), []api.ValueType{i32, i32}, []api.ValueType{i64}).
		Export("storage_len").
		NewFunctionBuilder().
		WithGoModuleFunction(api.GoModuleFunc(storageRead), []api.ValueType{i32, i32, i32, i32}, []api.ValueType{i64}).
		Export("storage_read").
		NewFunctionBuilder().
		WithGoModuleFunction(api.GoModuleFunc(abortFn), []api.ValueType{i32, i32, i32}, nil).
		Export("abort").
		Instantiate(ctx)
}

// gasGet reads the calling module's gas ledger via its own exported __zenon_gas_get accessor,
// the only route to the non-exported __zenon_gas global.
func gasGet(ctx context.Context, mod api.Module) (int64, error) {
	fn := mod.ExportedFunction("__zenon_gas_get")
	if fn == nil {
		return 0, fmt.Errorf("engine: module has no __zenon_gas_get export")
	}
	results, err := fn.Call(ctx)
	if err != nil {
		return 0, fmt.Errorf("engine: __zenon_gas_get: %w", err)
	}
	return int64(results[0]), nil
}

// gasSet writes the calling module's gas ledger via its own exported __zenon_gas_set accessor.
func gasSet(ctx context.Context, mod api.Module, v int64) error {
	fn := mod.ExportedFunction("__zenon_gas_set")
	if fn == nil {
		return fmt.Errorf("engine: module has no __zenon_gas_set export")
	}
	if _, err := fn.Call(ctx, api.EncodeI64(v)); err != nil {
		return fmt.Errorf("engine: __zenon_gas_set: %w", err)
	}
	return nil
}

// chargeGas is the single charge discipline, called first in every host function body -- and
// only from there, where wazero's recover converts a panic into an error on api.Function.Call.
func chargeGas(ctx context.Context, mod api.Module, cost uint64) {
	g, err := gasGet(ctx, mod)
	if err != nil {
		panic(&internalFault{err})
	}
	if g < 0 || uint64(g) < cost {
		if err := gasSet(ctx, mod, -1); err != nil {
			panic(&internalFault{err})
		}
		panic(errHostOutOfGas)
	}
	if err := gasSet(ctx, mod, g-int64(cost)); err != nil {
		panic(&internalFault{err})
	}
}

// storageLen implements env.storage_len(key_ptr, key_len) -> i64.
func storageLen(ctx context.Context, mod api.Module, stack []uint64) {
	chargeGas(ctx, mod, module.HostCost(module.HostStorageLen, 0))

	keyPtr := api.DecodeU32(stack[0])
	keyLen := api.DecodeU32(stack[1])
	if keyLen > codec.MaxStateKeySize {
		panic(fmt.Errorf("engine: storage_len: key_len %d exceeds MaxStateKeySize %d", keyLen, codec.MaxStateKeySize))
	}
	key, ok := mod.Memory().Read(keyPtr, keyLen)
	if !ok {
		panic(fmt.Errorf("engine: storage_len: key [%d, %d) out of bounds", keyPtr, uint64(keyPtr)+uint64(keyLen)))
	}

	hc := hostContext(ctx)
	n, present, err := hc.State.Len(keys.Derive(hc.ContractID, key))
	if err != nil {
		panic(&internalFault{err})
	}
	if !present {
		stack[0] = api.EncodeI64(-1)
		return
	}
	stack[0] = api.EncodeI64(int64(n))
}

// storageRead implements env.storage_read(key_ptr, key_len, out_ptr, out_len) -> i64.
func storageRead(ctx context.Context, mod api.Module, stack []uint64) {
	chargeGas(ctx, mod, module.HostCost(module.HostStorageRead, 0))

	keyPtr := api.DecodeU32(stack[0])
	keyLen := api.DecodeU32(stack[1])
	outPtr := api.DecodeU32(stack[2])
	outLen := api.DecodeU32(stack[3])

	if keyLen > codec.MaxStateKeySize {
		panic(fmt.Errorf("engine: storage_read: key_len %d exceeds MaxStateKeySize %d", keyLen, codec.MaxStateKeySize))
	}
	key, ok := mod.Memory().Read(keyPtr, keyLen)
	if !ok {
		panic(fmt.Errorf("engine: storage_read: key [%d, %d) out of bounds", keyPtr, uint64(keyPtr)+uint64(keyLen)))
	}

	hc := hostContext(ctx)
	value, present, err := hc.State.Read(keys.Derive(hc.ContractID, key))
	if err != nil {
		panic(&internalFault{err})
	}
	if !present {
		stack[0] = api.EncodeI64(-1)
		return
	}

	l := len(value)
	chargeGas(ctx, mod, module.HostCost(module.HostStorageRead, l)-module.HostCost(module.HostStorageRead, 0))

	if uint64(outLen) < uint64(l) {
		stack[0] = api.EncodeI64(-2)
		return
	}
	if uint64(outPtr)+uint64(outLen) > uint64(mod.Memory().Size()) {
		panic(fmt.Errorf("engine: storage_read: out buffer [%d, %d) out of bounds", outPtr, uint64(outPtr)+uint64(outLen)))
	}
	if !mod.Memory().Write(outPtr, value) {
		panic(fmt.Errorf("engine: storage_read: writing %d bytes at %d out of bounds", l, outPtr))
	}
	stack[0] = api.EncodeI64(int64(l))
}

// abortFn implements env.abort(code, data_ptr, data_len); it never returns. The charge comes
// first, so an unaffordable data_len is OUT_OF_GAS before the size check or any memory read.
// A data_len above codec.MaxReturnData raises the too-large abort without touching memory; a
// data_len of 0 never reads memory, so any data_ptr is accepted with it.
func abortFn(ctx context.Context, mod api.Module, stack []uint64) {
	code := api.DecodeI32(stack[0])
	dataPtr := api.DecodeU32(stack[1])
	dataLen := api.DecodeU32(stack[2])

	chargeGas(ctx, mod, module.HostCostU32(module.HostAbort, dataLen))
	if dataLen > codec.MaxReturnData {
		panic(&abortError{tooLarge: true})
	}
	if dataLen == 0 {
		panic(&abortError{code: code})
	}
	view, ok := mod.Memory().Read(dataPtr, dataLen)
	if !ok {
		panic(fmt.Errorf("engine: abort: data [%d, %d) out of bounds", dataPtr, uint64(dataPtr)+uint64(dataLen)))
	}
	panic(&abortError{code: code, data: append([]byte(nil), view...)})
}
