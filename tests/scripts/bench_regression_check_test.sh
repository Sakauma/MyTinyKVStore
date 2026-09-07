#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script_path="${repo_root}/scripts/bench-regression-check.sh"
test_root="$(mktemp -d)"
trap 'rm -rf -- "${test_root}"' EXIT

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

require_contains() {
  local text="$1"
  local expected="$2"
  [[ "${text}" == *"${expected}"* ]] || fail "expected output to contain: ${expected}"
}

run_case() {
  local name="$1"
  local statuses="$2"
  local expected_status="$3"
  local expected_candidates="$4"
  local expected_summary="$5"
  local expected_warning="$6"
  local sample_count="$7"
  local benchmark_status="$8"
  local case_root="${test_root}/${name}"
  mkdir -p "${case_root}/bin" "${case_root}/build-release/target/bin" "${case_root}/tmp"
  printf '{}\n' > "${case_root}/baseline.json"

  cat > "${case_root}/bin/cmake" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  chmod +x "${case_root}/bin/cmake"

  cat > "${case_root}/build-release/target/bin/kv_test" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  bench-baseline-json)
    if [[ "${KVSTORE_TEST_BENCHMARK_STATUS}" != "0" ]]; then
      echo "fixture benchmark error" >&2
      exit "${KVSTORE_TEST_BENCHMARK_STATUS}"
    fi
    printf '{"candidate":true}\n'
    ;;
  compare-baseline)
    candidate_path="$3"
    sample_index="${candidate_path##*-sample-}"
    sample_index="${sample_index%.json}"
    IFS=',' read -r -a statuses <<< "${KVSTORE_TEST_COMPARE_STATUSES}"
    status="${statuses[$((sample_index - 1))]}"
    echo "fixture_sample=${sample_index} fixture_status=${status}"
    exit "${status}"
    ;;
  *)
    exit 1
    ;;
esac
EOF
  chmod +x "${case_root}/build-release/target/bin/kv_test"

  set +e
  args=(baseline.json candidates 85 85 125)
  if [[ "${sample_count}" != "default" ]]; then
    args+=("${sample_count}")
  fi
  output="$(
    cd "${case_root}"
    PATH="${case_root}/bin:${PATH}" \
      KVSTORE_BENCHMARK_TMPDIR="${case_root}/tmp" \
      KVSTORE_TEST_COMPARE_STATUSES="${statuses}" \
      KVSTORE_TEST_BENCHMARK_STATUS="${benchmark_status}" \
      bash "${script_path}" "${args[@]}" 2>&1
  )"
  status=$?
  set -e

  [[ "${status}" -eq "${expected_status}" ]] ||
    fail "${name}: expected status ${expected_status}, got ${status}: ${output}"
  candidate_count="$(find "${case_root}/candidates" -maxdepth 1 -type f -name '*.json' | wc -l)"
  [[ "${candidate_count}" -eq "${expected_candidates}" ]] ||
    fail "${name}: expected ${expected_candidates} candidates, got ${candidate_count}"
  unique_count="$(find "${case_root}/candidates" -maxdepth 1 -type f -name '*.json' -printf '%f\n' | sort -u | wc -l)"
  [[ "${unique_count}" -eq "${candidate_count}" ]] || fail "${name}: candidate names are not unique"
  require_contains "${output}" "${expected_summary}"
  if [[ "${expected_warning}" == "yes" ]]; then
    require_contains "${output}" "warning: aggregate benchmark gate passed"
  elif [[ "${output}" == *"warning: aggregate benchmark gate passed"* ]]; then
    fail "${name}: unexpected aggregate warning"
  fi
  echo "[PASS] ${name}"
}

run_case one_of_three "0,2,2" 2 3 \
  "aggregate_samples=3 pass_count=1 gate_fail_count=2 required_pass_count=2 status=fail" no 3 0
run_case two_of_three "2,0,0" 0 3 \
  "aggregate_samples=3 pass_count=2 gate_fail_count=1 required_pass_count=2 status=pass" yes 3 0
run_case three_of_three "0,0,0" 0 3 \
  "aggregate_samples=3 pass_count=3 gate_fail_count=0 required_pass_count=2 status=pass" no 3 0
run_case comparator_error "1,0,0" 1 1 \
  "benchmark sample 1 comparison failed with status 1" no 3 0
run_case benchmark_error "0,0,0" 2 0 \
  "benchmark sample 1 generation failed with status 2" no 3 2
run_case default_single_sample "0" 0 1 \
  "aggregate_samples=1 pass_count=1 gate_fail_count=0 required_pass_count=1 status=pass" no default 0

echo "bench_regression_check_test: 6 passed"
