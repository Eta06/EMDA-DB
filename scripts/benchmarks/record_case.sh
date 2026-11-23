# Source from a GitHub Actions benchmark step.
mkdir -p "${RUNNER_TEMP:?}/benchmark-attempts"

record_benchmark() {
    local name=$1
    shift
    python3 -m scripts.benchmarks.record_case \
        --directory "$RUNNER_TEMP/benchmark-attempts/$name" --case "$name" -- "$@"
}
