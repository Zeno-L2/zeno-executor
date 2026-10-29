package zenoexec

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/module"
)

// declaredSentinels returns every top-level Err* var the files declare.
func declaredSentinels(t *testing.T, files ...string) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	out := map[string]bool{}
	for _, f := range files {
		af, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", f, err)
		}
		for _, d := range af.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.VAR {
				continue
			}
			for _, s := range gd.Specs {
				for _, n := range s.(*ast.ValueSpec).Names {
					if strings.HasPrefix(n.Name, "Err") {
						out[n.Name] = true
					}
				}
			}
		}
	}
	return out
}

func TestKindTableCoversEverySentinel(t *testing.T) {
	declared := declaredSentinels(t, "../internal/module/errors.go", "../internal/module/instrument.go")
	if len(declared) == 0 {
		t.Fatal("no sentinels parsed")
	}

	inTable := map[string]bool{}
	kinds := map[string]string{}
	for _, e := range moduleKinds {
		if inTable[e.name] {
			t.Errorf("table lists %s twice", e.name)
		}
		inTable[e.name] = true
		if !declared[e.name] {
			t.Errorf("table name %s is declared in no module file", e.name)
		}
		if prev, dup := kinds[e.kind]; dup {
			t.Errorf("kind %q is used by %s and %s", e.kind, prev, e.name)
		}
		kinds[e.kind] = e.name
		if e.sentinel == nil {
			t.Errorf("table entry %s has a nil sentinel", e.name)
		}
	}
	for name := range declared {
		if !inTable[name] {
			t.Errorf("sentinel %s has no kind", name)
		}
	}
}

func TestKind(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"EngineCompile", wasm.ErrEngineCompile, "engine_compile"},
		{"DerivePanic", fmt.Errorf("x: %w", wasm.ErrDerivePanic), "internal_panic"},
		{"InvalidOwner", fmt.Errorf("x: %w", codec.ErrInvalidOwner), "invalid_metadata"},
		{"InvalidPolicy", codec.ErrInvalidPolicy, "invalid_metadata"},
		{"UnknownVersion", codec.ErrUnknownVersion, "invalid_metadata"},
		{"Float", fmt.Errorf("x: %w", module.ErrFloatType), "float_type"},
		{"SIMD", module.ErrSIMD, "simd"},
		{"LEB128", module.ErrNonCanonicalLEB128, "non_canonical_leb128"},
		{"Unrelated", errors.New("other"), "unknown"},
	}
	for _, tc := range cases {
		if got := Kind(tc.err); got != tc.want {
			t.Errorf("%s: Kind = %q, want %q", tc.name, got, tc.want)
		}
	}
}
