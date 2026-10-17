package module

import (
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestErrorCoverage scans errors.go for every declared reason and
// asserts two things: every scanned name is registered in the
// hand-written map below (a new reason was declared and not
// registered), and every registered value is errors.Is-matched by at
// least one rejectionCases entry (a declared reason no fixture
// exercises). This is what stops the error set from silently drifting
// away from the cases that cover it.
func TestErrorCoverage(t *testing.T) {
	known := map[string]error{
		"ErrModuleTooLarge":              ErrModuleTooLarge,
		"ErrBadMagic":                    ErrBadMagic,
		"ErrBadVersion":                  ErrBadVersion,
		"ErrDuplicateSection":            ErrDuplicateSection,
		"ErrSectionOrder":                ErrSectionOrder,
		"ErrUnknownSection":              ErrUnknownSection,
		"ErrCustomSection":               ErrCustomSection,
		"ErrTruncated":                   ErrTruncated,
		"ErrSectionSizeMismatch":         ErrSectionSizeMismatch,
		"ErrTrailingBytes":               ErrTrailingBytes,
		"ErrStartSection":                ErrStartSection,
		"ErrMalformedSection":            ErrMalformedSection,
		"ErrMultiValueResult":            ErrMultiValueResult,
		"ErrFloatType":                   ErrFloatType,
		"ErrSIMD":                        ErrSIMD,
		"ErrReferenceType":               ErrReferenceType,
		"ErrInvalidValType":              ErrInvalidValType,
		"ErrDisallowedImport":            ErrDisallowedImport,
		"ErrImportSignature":             ErrImportSignature,
		"ErrTypeIndexOutOfBounds":        ErrTypeIndexOutOfBounds,
		"ErrTooManyTables":               ErrTooManyTables,
		"ErrInvalidLimits":               ErrInvalidLimits,
		"ErrTableTooLarge":               ErrTableTooLarge,
		"ErrTooManyMemories":             ErrTooManyMemories,
		"ErrSharedMemory":                ErrSharedMemory,
		"ErrMemoryTooLarge":              ErrMemoryTooLarge,
		"ErrInvalidMutability":           ErrInvalidMutability,
		"ErrNonConstantInitializer":      ErrNonConstantInitializer,
		"ErrMissingExport":               ErrMissingExport,
		"ErrFunctionIndexOutOfBounds":    ErrFunctionIndexOutOfBounds,
		"ErrRunImported":                 ErrRunImported,
		"ErrRunSignature":                ErrRunSignature,
		"ErrReservedExport":              ErrReservedExport,
		"ErrMutableGlobalExport":         ErrMutableGlobalExport,
		"ErrBulkMemory":                  ErrBulkMemory,
		"ErrNonConstantSegmentOffset":    ErrNonConstantSegmentOffset,
		"ErrTableIndexOutOfBounds":       ErrTableIndexOutOfBounds,
		"ErrMemoryIndexOutOfBounds":      ErrMemoryIndexOutOfBounds,
		"ErrDataSegmentOutOfBounds":      ErrDataSegmentOutOfBounds,
		"ErrElementSegmentOutOfBounds":   ErrElementSegmentOutOfBounds,
		"ErrFunctionCodeMismatch":        ErrFunctionCodeMismatch,
		"ErrTooManyLocals":               ErrTooManyLocals,
		"ErrTotalLocalsExceeded":         ErrTotalLocalsExceeded,
		"ErrTooManyParams":               ErrTooManyParams,
		"ErrTotalParamsExceeded":         ErrTotalParamsExceeded,
		"ErrMalformedFunctionBody":       ErrMalformedFunctionBody,
		"ErrExceptionHandling":           ErrExceptionHandling,
		"ErrTypedSelect":                 ErrTypedSelect,
		"ErrFloatInstruction":            ErrFloatInstruction,
		"ErrSignExtension":               ErrSignExtension,
		"ErrSaturatingConversion":        ErrSaturatingConversion,
		"ErrAtomics":                     ErrAtomics,
		"ErrUnmeteredOpcode":             ErrUnmeteredOpcode,
		"ErrGlobalIndexOutOfBounds":      ErrGlobalIndexOutOfBounds,
		"ErrLabelOutOfBounds":            ErrLabelOutOfBounds,
		"ErrLocalIndexOutOfBounds":       ErrLocalIndexOutOfBounds,
		"ErrBlockTypeIndex":              ErrBlockTypeIndex,
		"ErrNonCanonicalLEB128":          ErrNonCanonicalLEB128,
		"ErrIntegerTooLarge":             ErrIntegerTooLarge,
		"ErrTooManyTypes":                ErrTooManyTypes,
		"ErrTooManyFunctions":            ErrTooManyFunctions,
		"ErrTooManyGlobals":              ErrTooManyGlobals,
		"ErrTooManyDataSegments":         ErrTooManyDataSegments,
		"ErrTotalElementEntriesExceeded": ErrTotalElementEntriesExceeded,
	}

	raw, err := os.ReadFile("errors.go")
	if err != nil {
		t.Fatalf("reading errors.go: %v", err)
	}
	re := regexp.MustCompile(`(?m)^var (Err[A-Za-z0-9]+)\b`)
	matches := re.FindAllStringSubmatch(string(raw), -1)
	if len(matches) == 0 {
		t.Fatal("no `var ErrX` lines found in errors.go; did the declaration shape change?")
	}
	scanned := make(map[string]bool, len(matches))
	for _, m := range matches {
		scanned[m[1]] = true
	}

	for name := range scanned {
		if _, ok := known[name]; !ok {
			t.Errorf("errors.go declares %s, which is not registered in this test's known map", name)
		}
	}
	for name := range known {
		if !scanned[name] {
			t.Errorf("known map registers %s, which errors.go no longer declares", name)
		}
	}

	for name, want := range known {
		found := false
		for _, c := range rejectionCases {
			if errors.Is(c.want, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s is declared but no rejectionCases entry asserts it", name)
		}
	}
}

// TestImportBoundary parses every .go file in this package directory (including _test.go)
// and asserts each import path is either standard library or begins with
// github.com/digitalSloth/go-executor/: the mechanical form of "internal/module never
// imports wazero", following the precedent of scripts/check-trie-imports.sh.
func TestImportBoundary(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("globbing *.go: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no .go files found in package directory")
	}
	fset := token.NewFileSet()
	for _, f := range files {
		astFile, err := parser.ParseFile(fset, f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parsing %s: %v", f, err)
		}
		for _, imp := range astFile.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("%s: unquoting import %s: %v", f, imp.Path.Value, err)
			}
			if allowedImport(path) {
				continue
			}
			t.Errorf("%s imports %q, which is neither standard library nor github.com/digitalSloth/go-executor/...", f, path)
		}
	}
}

func allowedImport(path string) bool {
	if strings.HasPrefix(path, "github.com/digitalSloth/go-executor/") {
		return true
	}
	first := path
	if i := strings.Index(path, "/"); i >= 0 {
		first = path[:i]
	}
	return !strings.Contains(first, ".")
}
