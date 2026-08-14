#!/usr/bin/env bash
# ============================================================
# Fusion Plugin Builder
#
# Builds all capability and unit plugin crates as dynamic
# libraries and copies the outputs to the app lib directories:
#
#   app/assets/libs/capability/   ← capability dylibs
#   app/assets/libs/unit/         ← unit dylibs
#
# Usage:
#   sh scripts/build-plugins.sh              # debug build
#   sh scripts/build-plugins.sh --release    # release build
#   sh scripts/build-plugins.sh --help
#
# Naming convention:
#   fusion-capability-*  → capability dylib
#   fusion-unit-*        → unit dylib
# ============================================================

set -eo pipefail
# NOTE: set -u is NOT used because empty arrays trigger "unbound variable"
# in some bash versions. Array emptiness is checked explicitly where needed.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---- Defaults ----
PROFILE="debug"
TARGET_DIR="target/debug"
ONLY_CAPABILITIES=false
ONLY_UNITS=false
ONLY_CRATES=()
SKIP_CRATES=()
VERBOSE=false

# ---- Parse args ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --release)
            PROFILE="release"
            TARGET_DIR="target/release"
            shift
            ;;
        --only-capabilities)
            ONLY_CAPABILITIES=true
            shift
            ;;
        --only-units)
            ONLY_UNITS=true
            shift
            ;;
        --only)
            IFS=',' read -ra CRATES <<< "$2"
            ONLY_CRATES+=("${CRATES[@]}")
            shift 2
            ;;
        --skip)
            IFS=',' read -ra CRATES <<< "$2"
            SKIP_CRATES+=("${CRATES[@]}")
            shift 2
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Fusion Plugin Builder"
            echo ""
            echo "Usage: bash scripts/build-plugins.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --release               Release build"
            echo "  --only-capabilities     Build only capability crates"
            echo "  --only-units            Build only unit crates"
            echo "  --only A,B,C            Build only specified crates (comma-separated)"
            echo "  --skip A,B,C            Skip specified crates"
            echo "  --verbose, -v           Show full cargo output"
            echo "  --help, -h              Show this help"
            echo ""
            echo "By default, builds all capability and unit crates."
            echo "Cargo handles incremental compilation automatically —"
            echo "unchanged crates are not rebuilt."
            exit 0
            ;;
        *)
            echo "Unknown option: $1 (use --help)"
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"

echo "=========================================="
echo " Fusion Plugin Builder ($PROFILE)"
echo "=========================================="

# ---- Collect crates by classification ----
CAPABILITY_CRATES=()
UNIT_CRATES=()
ALL_CRATES=()
CARGO_ARGS=()

# Infrastructure crates matching the pattern but NOT plugins.
NON_PLUGIN_CRATES="fusion-unit-sdk fusion-unit-tests"

cargo metadata --format-version 1 --no-deps 2>/dev/null \
    | grep -oE '"name":"[^"]*"' \
    | sed 's/"name":"//;s/"//' \
    | grep -E '^fusion-(capability|unit)-' \
    | sort -u \
    > /tmp/fusion-crates.txt

while IFS= read -r crate; do
    if [ -z "$crate" ]; then continue; fi

    # Skip infrastructure crates.
    if echo "$NON_PLUGIN_CRATES" | grep -qw "$crate"; then
        echo "  [SKIP] $crate (infrastructure)"
        continue
    fi

    case "$crate" in
        fusion-capability-*) CAPABILITY_CRATES+=("$crate") ;;
        fusion-unit-*)       UNIT_CRATES+=("$crate") ;;
    esac
done < /tmp/fusion-crates.txt
rm -f /tmp/fusion-crates.txt

echo ""
echo "Capability crates (${#CAPABILITY_CRATES[@]}):"
for c in "${CAPABILITY_CRATES[@]}"; do echo "  - $c"; done

echo ""
echo "Unit crates (${#UNIT_CRATES[@]}):"
for c in "${UNIT_CRATES[@]}"; do echo "  - $c"; done

# ---- Apply filters ----
if [[ "$ONLY_CAPABILITIES" == true ]]; then
    UNIT_CRATES=()
fi
if [[ "$ONLY_UNITS" == true ]]; then
    CAPABILITY_CRATES=()
fi
if [[ ${#ONLY_CRATES[@]} -gt 0 ]]; then
    CAPABILITY_CRATES=($(printf '%s\n' "${CAPABILITY_CRATES[@]}" | grep -F -f <(printf '%s\n' "${ONLY_CRATES[@]}") || true))
    UNIT_CRATES=($(printf '%s\n' "${UNIT_CRATES[@]}" | grep -F -f <(printf '%s\n' "${ONLY_CRATES[@]}") || true))
fi
if [[ ${#SKIP_CRATES[@]} -gt 0 ]]; then
    CAPABILITY_CRATES=($(printf '%s\n' "${CAPABILITY_CRATES[@]}" | grep -vF -f <(printf '%s\n' "${SKIP_CRATES[@]}") || true))
    UNIT_CRATES=($(printf '%s\n' "${UNIT_CRATES[@]}" | grep -vF -f <(printf '%s\n' "${SKIP_CRATES[@]}") || true))
fi

echo ""
echo "After filtering:"
echo "  Capability (${#CAPABILITY_CRATES[@]}): ${CAPABILITY_CRATES[*]:-none}"
echo "  Unit       (${#UNIT_CRATES[@]}): ${UNIT_CRATES[*]:-none}"

# ---- Build ----
CARGO_FLAGS=""
if [[ "$PROFILE" == "release" ]]; then
    CARGO_FLAGS="--release"
fi

ALL_CRATES=("${CAPABILITY_CRATES[@]}" "${UNIT_CRATES[@]}")

if [[ ${#ALL_CRATES[@]} -eq 0 ]]; then
    echo ""
    echo "No plugin crates found. Nothing to build."
    exit 0
fi

# Build each plugin crate in its own cargo invocation. A single
# invocation with multiple `-p` unifies features across the selected
# packages: `-p fusion-unit-datafusion` enables its default `cdylib`
# feature, and that union leaks into crates that depend on it with
# `default-features = false` (e.g. provider dylibs) — their binary
# images would accidentally export `init_plugin` / `init_capability_plugin`
# and override the unit registration at load time. Per-crate invocations
# respect each crate's own declared features; cargo still shares
# incremental artifacts in the target dir.
echo ""
echo "Building ${#ALL_CRATES[@]} plugin crate(s)..."

for crate in "${ALL_CRATES[@]}"; do
    if [[ "$VERBOSE" == true ]]; then
        cargo build -p "$crate" $CARGO_FLAGS 2>&1 || exit 1
    else
        # Filter output to show per-crate progress without drowning
        # in warnings. pipefail surfaces cargo's exit code; grep's own
        # non-zero (no matching lines) is harmless here — a successful
        # build always prints at least "Finished".
        cargo build -p "$crate" $CARGO_FLAGS 2>&1 \
            | grep --line-buffered -E '^\s*(Compiling|Building|Finished|error(\[|:))' \
            || exit 1
    fi
done

echo ""
echo "All crates built."

# ---- Copy dylibs ----
PLATFORM_EXT=""
case "$(uname -s)" in
    Darwin)  PLATFORM_EXT="dylib" ;;
    Linux)   PLATFORM_EXT="so" ;;
    MINGW*|MSYS*|CYGWIN*) PLATFORM_EXT="dll" ;;
    *)       echo "Unknown platform"; exit 1 ;;
esac

copy_dylib() {
    local crate_name="$1"
    local target_dir="$2"

    # Cargo replaces hyphens with underscores in the linker library name.
    local lib_name="${crate_name//-/_}"
    local src="$TARGET_DIR/lib${lib_name}.${PLATFORM_EXT}"

    if [[ -f "$src" ]]; then
        mkdir -p "$target_dir"
        # Strip local symbols + debug info at copy time — a debug build
        # carries ~150MB of symbol table (e.g. 405k local symbols) even
        # though the app only calls the FFI exports at runtime. The
        # unstripped copy stays in target/ for debugging.
        if [[ "$PLATFORM_EXT" != "dll" ]]; then
            strip -x "$src" -o "$target_dir/lib${lib_name}.${PLATFORM_EXT}"
        else
            cp "$src" "$target_dir/"
        fi
        echo "  $crate_name → $target_dir/"
    else
        echo "  [WARN] $crate_name: dylib not found at $src (crate-type may be missing cdylib)"
    fi
}

echo ""
echo "Installing capability dylibs..."
for crate in "${CAPABILITY_CRATES[@]}"; do
    copy_dylib "$crate" "app/assets/libs/capability"
done

echo ""
echo "Installing unit dylibs..."
for crate in "${UNIT_CRATES[@]}"; do
    copy_dylib "$crate" "app/assets/libs/unit"
done

# ---- Verify crate-type ----
# Remind the user to check [lib] crate-type if a dylib wasn't produced.
echo ""
echo "=========================================="
echo " Done."
echo " Capability dylibs → app/assets/libs/capability/"
echo " Unit dylibs       → app/assets/libs/unit/"
echo ""
echo " If a crate is missing above, ensure it has:"
echo "   [lib]"
echo "   crate-type = [\"cdylib\", \"lib\"]"
echo "=========================================="
