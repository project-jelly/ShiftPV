#!/bin/sh
# Qualify an immutable helper image in one --init container, with a 250-second
# run and at most 50-second cleanup budget. No host mounts or host networking.
# apk needs temporary bridge access; all six qualification cases run after the
# container has been disconnected, using only its own loopback TCP connection.
set -eu

probe_root=/tmp/shiftpv-032-final
source_dir=${probe_root}/source
password=shiftpv-032-disposable-proof

fail() { printf 'FAIL %s\n' "$*" >&2; exit 1; }

runtime_fingerprint() {
  prefix=$1
  binary=$(command -v rsync) || return 1
  binary=$(readlink -f "${binary}") || return 1
  test -f "${binary}" || return 1
  printf '%s\n' "${binary}" >"${probe_root}/${prefix}.paths" || return 1
  # Do not pipe hash/ldd failures through a later successful sort/while stage.
  ldd "${binary}" >"${probe_root}/${prefix}.ldd" 2>&1 || return 1
  awk '/not found|Error loading shared library|Error relocating/ {bad=1}
    {for (i=1;i<=NF;i++) if ($i ~ /^\//) {print $i; count++}}
    END {if (bad || count==0) exit 1}' "${probe_root}/${prefix}.ldd" >>"${probe_root}/${prefix}.paths" || return 1
  sort -u "${probe_root}/${prefix}.paths" >"${probe_root}/${prefix}.sorted" || return 1
  : >"${probe_root}/${prefix}.sha256" || return 1
  while IFS= read -r dependency; do
    resolved=$(readlink -f "${dependency}") || return 1
    test -f "${resolved}" || return 1
    sha256sum "${resolved}" >>"${probe_root}/${prefix}.sha256" || return 1
  done <"${probe_root}/${prefix}.sorted"
}

prepare_tools() {
  test -f /etc/alpine-release || fail 'candidate is not Alpine'
  mkdir -p "${probe_root}"
  rsync --version >"${probe_root}/version.txt"
  grep -q 'zstd' "${probe_root}/version.txt" || fail 'zstd unsupported'
  runtime_fingerprint runtime-before || fail 'cannot fingerprint original runtime'
  binary=$(command -v rsync) || fail 'rsync absent'
  readlink -f "${binary}" >"${probe_root}/original-rsync-path" || fail 'cannot resolve original rsync'
  # Fingerprint the original executable and every resolved shared dependency,
  # including codecs. Refuse qualification if installing test tools changes any.
  apk add --no-cache acl attr
  runtime_fingerprint runtime-after || fail 'cannot fingerprint installed runtime'
  cmp "${probe_root}/runtime-before.sha256" "${probe_root}/runtime-after.sha256" || fail 'candidate runtime changed during setup'
  for tool in setsid setfacl getfacl setfattr getfattr sha256sum; do command -v "${tool}" >/dev/null || fail "missing ${tool}"; done
  printf 'RUNTIME_UNCHANGED\n'
  cat "${probe_root}/runtime-after.sha256"
}

build_fixture() {
  mkdir -p "${source_dir}/nested/empty"
  dd if=/dev/urandom of="${source_dir}/00-dense" bs=1048576 count=16 2>/dev/null
  truncate -s 16777216 "${source_dir}/sparse"
  # Both leading and trailing holes; random islands prevent an all-zero fixture.
  dd if="${source_dir}/00-dense" of="${source_dir}/sparse" bs=4096 count=1 seek=256 conv=notrunc 2>/dev/null
  dd if="${source_dir}/00-dense" of="${source_dir}/sparse" bs=4096 count=1 skip=1 seek=2048 conv=notrunc 2>/dev/null
  printf 'shiftpv-032-metadata\n' >"${source_dir}/nested/data"
  chmod 0640 "${source_dir}/nested/data"
  chown 1234:2345 "${source_dir}/nested/data"
  ln "${source_dir}/nested/data" "${source_dir}/hardlink"
  ln -s nested/data "${source_dir}/symlink"
  mkfifo "${source_dir}/fifo"
  setfattr -n user.shiftpv.proof -v source-032 "${source_dir}/nested/data"
  setfacl -m u:1235:r-- "${source_dir}/nested/data"
  sync
  (cd "${source_dir}" && sha256sum 00-dense sparse nested/data) >"${probe_root}/source.sha256"
  printf 'shiftpv:%s\n' "${password}" >"${probe_root}/secrets"
  chmod 0600 "${probe_root}/secrets"
  cat >"${probe_root}/rsyncd.conf" <<CONFIG
uid = 0
gid = 0
use chroot = no
read only = yes
strict modes = yes
port = 1873
address = 127.0.0.1
[data]
path = ${source_dir}
auth users = shiftpv
secrets file = ${probe_root}/secrets
CONFIG
  printf 'FIXTURE dense_bytes=16777216 sparse_bytes=16777216 sparse_blocks=%s\n' "$(stat -c '%b' "${source_dir}/sparse")"
  cat "${probe_root}/source.sha256"
}

copy_files() {
  mode=$1 destination=$2
  set -- -aHAXS --numeric-ids --one-file-system --no-devices --delete --fsync --stats --bwlimit=1024
  if [ "${mode}" = zstd1 ]; then
    set -- "$@" --compress --compress-choice=zstd --compress-level=1
  fi
  # The rate cap creates an observable partial file, not a latency benchmark.
  # No --partial, --inplace or alternate verification mode is introduced.
  RSYNC_PASSWORD="${password}" exec rsync "$@" rsync://shiftpv@127.0.0.1:1873/data/ "${destination}/"
}

verify_files() {
  destination=$1
  (cd "${destination}" && sha256sum -c "${probe_root}/source.sha256")
  test "$(stat -c '%a' "${destination}/nested/data")" = 640
  test "$(stat -c '%u:%g' "${destination}/nested/data")" = 1234:2345
  test "$(readlink "${destination}/symlink")" = nested/data
  test -p "${destination}/fifo"
  test -d "${destination}/nested/empty"
  test "$(stat -c '%i' "${destination}/nested/data")" = "$(stat -c '%i' "${destination}/hardlink")"
  test "$(stat -c '%s' "${destination}/00-dense")" = 16777216
  test "$(stat -c '%s' "${destination}/sparse")" = 16777216
  test "$(stat -c '%b' "${destination}/sparse")" -lt 1024
  test "$(getfattr --only-values -n user.shiftpv.proof "${destination}/nested/data" 2>/dev/null)" = source-032
  getfacl -cp "${destination}/nested/data" | grep -Fxq 'user:1235:r--'
  test ! -e "${destination}/obsolete"
  dry_run=$(RSYNC_PASSWORD="${password}" rsync -aHAXS --numeric-ids --one-file-system --no-devices \
    --delete --checksum --dry-run --itemize-changes rsync://shiftpv@127.0.0.1:1873/data/ "${destination}/")
  test -z "${dry_run}" || fail "checksum dry-run reports differences: ${dry_run}"
  printf 'METADATA_PASS destination=%s sha256=equal checksum_dryrun=empty sparse_blocks=%s\n' \
    "${destination##*/}" "$(stat -c '%b' "${destination}/sparse")"
}

monotonic_ms() {
  awk '{printf "%.0f\n", $1 * 1000}' /proc/uptime
}

before_deadline() {
  current_ms=$(monotonic_ms) || return 1
  test "${current_ms}" -lt "$1"
}

proc_fields() {
  # /proc/PID/stat: discard the parenthesized comm before reading state/ppid/pgrp/session.
  raw=$(cat "/proc/$1/stat" 2>/dev/null) || return 1
  fields=${raw##*) }
  # /proc fields are kernel-produced whitespace-separated scalar values.
  # shellcheck disable=SC2086
  set -- ${fields}
  test "$#" -ge 4 || return 1
  printf '%s %s %s %s\n' "$1" "$2" "$3" "$4"
}

assert_member() {
  member_fields=$(proc_fields "$1") || fail "cannot inspect PID $1"
  read -r state _parent observed_group session <<FIELDS
${member_fields}
FIELDS
  if [ "${state}" = Z ] || [ "${observed_group}" != "$2" ] || [ "${session}" != "$2" ]; then
    fail "PID $1 is not live in verified PGID/SID $2"
  fi
}

assert_rsync_member() {
  assert_member "$1" "$2"
  executable=$(readlink -f "/proc/$1/exe") || fail "cannot resolve executable for PID $1"
  original=$(cat "${probe_root}/original-rsync-path") || fail 'original rsync identity absent'
  test "${executable}" = "${original}" || fail "PID $1 is not original rsync: ${executable}"
  printf 'RSYNC_MEMBER pid=%s pgid=%s sid=%s exe=%s\n' "$1" "$2" "$2" "${executable}"
}

assert_all_rsync_in_group() {
  original=$(cat "${probe_root}/original-rsync-path") || fail 'original rsync identity absent'
  for proc in /proc/[0-9]*; do
    executable=$(readlink -f "${proc}/exe" 2>/dev/null) || continue
    test "${executable}" = "${original}" || continue
    assert_member "${proc##*/}" "$1"
  done
}

group_present() {
  wanted=$1 include_zombie=$2
  for entry in /proc/[0-9]*/stat; do
    test -r "${entry}" || continue
    pid=${entry%/stat}; pid=${pid##*/}
    fields=$(proc_fields "${pid}") || continue # A process may exit during this read.
    # shellcheck disable=SC2086 # Kernel-produced state and numeric identity fields.
    set -- ${fields}
    if [ "$3" = "${wanted}" ] && { [ "${include_zombie}" = 1 ] || [ "$1" != Z ]; }; then return 0; fi
  done
  return 1
}

run_session() {
  mode=$1 destination=$2 attempt_dir=$3
  daemon_pid='' receiver_pid=''
  # shellcheck disable=SC2317,SC2329 # Invoked by the EXIT trap.
  session_cleanup() {
    rc=$?
    trap - EXIT HUP INT TERM
    if [ -n "${daemon_pid}" ]; then kill -TERM "${daemon_pid}" 2>/dev/null || true; wait "${daemon_pid}" 2>/dev/null || true; fi
    exit "${rc}"
  }
  trap session_cleanup EXIT
  trap 'exit 143' HUP INT TERM
  rsync --daemon --no-detach --config="${probe_root}/rsyncd.conf" >"${attempt_dir}/daemon.log" 2>&1 &
  daemon_pid=$!
  ready=0
  ready_started=$(monotonic_ms) || fail 'cannot read monotonic clock'
  ready_deadline=$((ready_started + 3000))
  while before_deadline "${ready_deadline}"; do
    if RSYNC_PASSWORD="${password}" rsync rsync://shiftpv@127.0.0.1:1873/data/ >/dev/null 2>&1; then ready=1; break; fi
    sleep 0.1
  done
  test "${ready}" = 1 || fail 'daemon never became ready'
  copy_files "${mode}" "${destination}" >"${attempt_dir}/copy.log" 2>&1 &
  receiver_pid=$!
  printf '%s %s %s\n' "$$" "${daemon_pid}" "${receiver_pid}" >"${attempt_dir}/members"
  wait "${receiver_pid}"
  # The interrupted attempt never reaches this point. A successful attempt must
  # satisfy metadata and the unchanged daemon-based production checksum verify.
  verify_files "${destination}"
}

start_group() {
  mode=$1 destination=$2 attempt_dir=$3
  mkdir -p "${attempt_dir}"
  setsid /bin/sh "$0" --session "${mode}" "${destination}" "${attempt_dir}" >"${attempt_dir}/session.log" 2>&1 &
  leader=$!
  active_group=${leader}
  setup_started=$(monotonic_ms) || fail 'cannot read monotonic clock'
  setup_deadline=$((setup_started + 10000))
  while [ ! -f "${attempt_dir}/members" ]; do
    kill -0 "${leader}" 2>/dev/null || fail 'session exited before reporting members'
    before_deadline "${setup_deadline}" || fail 'session setup exceeded 10s'
    sleep 0.1
  done
  read -r group daemon receiver <"${attempt_dir}/members"
  test "${group}" = "${leader}" || fail 'setsid forked: wait PID does not identify session leader'
  assert_member "${group}" "${group}"
  assert_rsync_member "${daemon}" "${group}"
  assert_rsync_member "${receiver}" "${group}"
  printf 'VERIFIED_GROUP pgid=%s sid=%s daemon=%s receiver=%s\n' "${group}" "${group}" "${daemon}" "${receiver}"
}

finish_group() {
  expected=$1 settle_deadline=$2
  # One monotonic deadline covers live exit, wait() and orphan/zombie reaping.
  # Do not wait on the leader until /proc proves that no group member is live.
  while group_present "${group}" 0; do
    before_deadline "${settle_deadline}" || fail 'group live past settlement deadline'
    sleep 0.1
  done
  before_deadline "${settle_deadline}" || fail 'group exhausted settlement deadline before wait'
  if wait "${leader}"; then rc=0; else rc=$?; fi
  if [ "${expected}" = interrupted ]; then test "${rc}" -ne 0 || fail 'interrupted copy reported success'; else test "${rc}" = 0 || fail "copy/verify exit ${rc}"; fi
  # --init adopts/reaps daemon and receiver orphans. Reject surviving zombies too.
  while group_present "${group}" 1; do
    before_deadline "${settle_deadline}" || fail 'group not fully reaped before settlement deadline'
    sleep 0.1
  done
  active_group=
  settled_ms=$(monotonic_ms) || fail 'cannot read monotonic clock'
  test "${settled_ms}" -lt "${settle_deadline}" || fail 'group reaping exceeded settlement deadline'
  printf 'GROUP_SETTLED pgid=%s exit=%s no_members=true uptime_ms=%s deadline_ms=%s\n' "${group}" "${rc}" "${settled_ms}" "${settle_deadline}"
}

interrupt_partial() {
  destination=$1 signal=$2 observed=0 partial_size=0
  partial_started=$(monotonic_ms) || fail 'cannot read monotonic clock'
  partial_deadline=$((partial_started + 15000))
  while before_deadline "${partial_deadline}"; do
    assert_rsync_member "${receiver}" "${group}"
    for partial in "${destination}"/.00-dense.*; do
      test -f "${partial}" || continue
      partial_size=$(stat -c '%s' "${partial}")
      if [ "${partial_size}" -gt 0 ] && [ "${partial_size}" -lt 16777216 ]; then observed=1; break; fi
    done
    test "${observed}" = 1 && break
    sleep 0.1
  done
  test "${observed}" = 1 || fail 'no actual partial dense file before interruption'
  assert_rsync_member "${daemon}" "${group}"
  assert_all_rsync_in_group "${group}"
  signal_ms=$(monotonic_ms) || fail 'cannot read monotonic clock'
  settle_deadline=$((signal_ms + 15000))
  printf 'INTERRUPT signal=%s pgid=%s actual_partial_bytes=%s uptime_ms=%s deadline_ms=%s\n' "${signal}" "${group}" "${partial_size}" "${signal_ms}" "${settle_deadline}"
  kill -"${signal}" "-${group}"
  finish_group interrupted "${settle_deadline}"
}

run_cases() {
  active_group=
  # shellcheck disable=SC2317,SC2329 # Invoked by the EXIT trap.
  cleanup_groups() {
    rc=$?
    trap - EXIT HUP INT TERM
    if [ -n "${active_group}" ]; then kill -KILL "-${active_group}" 2>/dev/null || true; fi
    exit "${rc}"
  }
  trap cleanup_groups EXIT
  trap 'exit 143' HUP INT TERM
  build_fixture
  for mode in baseline zstd1; do
    for outcome in clean TERM KILL; do
      case_dir=${probe_root}/${mode}-${outcome}
      destination=${case_dir}/destination
      mkdir -p "${destination}"
      printf 'remove me\n' >"${destination}/obsolete"
      start_group "${mode}" "${destination}" "${case_dir}/initial"
      if [ "${outcome}" = clean ]; then
        copy_started=$(monotonic_ms) || fail 'cannot read monotonic clock'
        finish_group success "$((copy_started + 65000))"
      else
        interrupt_partial "${destination}" "${outcome}"
        # Retry the same destination only after the entire earlier group exited.
        start_group "${mode}" "${destination}" "${case_dir}/retry"
        copy_started=$(monotonic_ms) || fail 'cannot read monotonic clock'
        finish_group success "$((copy_started + 65000))"
      fi
      printf 'CASE_PASS %s-%s\n' "${mode}" "${outcome}"
      cat "${case_dir}"/*/session.log
    done
  done
  printf 'QUALIFICATION_PASS cases=6\n'
}

case "${1:-}" in
  --prepare) prepare_tools; exit ;;
  --inside) run_cases; exit ;;
  --session) shift; run_session "$@"; exit ;;
esac

# Host orchestration uses one run deadline and a separate bounded cleanup window.
# No default/tag is accepted, and only this random ownership label authorizes rm.
[ "$#" -eq 1 ] || fail 'usage: filesystem-primitives.sh sha256:<64 hex>'
exec python3 - "$0" "$1" <<'PY'
import json, pathlib, re, signal, subprocess, sys, time, uuid

script = pathlib.Path(sys.argv[1]).resolve()
image = sys.argv[2]
if not re.fullmatch(r"sha256:[0-9a-f]{64}", image):
    raise SystemExit("candidate must be an immutable local image ID, not a tag")
started = time.monotonic()
run_deadline = started + 250
cleanup_deadline = None
name = "shiftpv-032-final-" + uuid.uuid4().hex
label_key = "shiftpv.qualification.032"
label = label_key + "=" + name
out = script.parents[2] / ".tmp" / "rsync-qualification" / name
out.mkdir(parents=True)
result = {"image": image, "container": name, "owner_label": label, "cases_expected": 6,
          "run_budget_seconds": 250, "cleanup_budget_seconds": 50, "total_budget_seconds": 300,
          "host_mounts": False, "host_network": False, "networks_created": [],
          "qualified": False, "container_absent": None}
container_id = None
interrupted = None

class Interrupted(RuntimeError):
    pass

def on_signal(signum, _frame):
    global interrupted
    interrupted = signum
    raise Interrupted("received " + signal.Signals(signum).name)

signal.signal(signal.SIGTERM, on_signal)
signal.signal(signal.SIGINT, on_signal)

def call(args, data=None, log=None, limit=None):
    deadline = cleanup_deadline if cleanup_deadline is not None else run_deadline
    budget = deadline - time.monotonic()
    if limit is not None:
        budget = min(budget, limit)
    if budget <= 0:
        raise TimeoutError("qualification time budget exhausted")
    if log is not None:
        with (out / log).open("wb") as stream:
            return subprocess.run(args, input=data, stdout=stream, stderr=subprocess.STDOUT,
                                  timeout=budget, check=True)
    return subprocess.run(args, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=budget, check=True)

def owned_metadata(identity, limit=5):
    current = json.loads(call(["docker", "container", "inspect", identity], limit=limit).stdout)[0]
    if current["Id"] != identity or current["Config"].get("Labels", {}).get(label_key) != name:
        raise RuntimeError("refusing foreign container identity or ownership label")
    return current

try:
    metadata = json.loads(call(["docker", "image", "inspect", image]).stdout)[0]
    if metadata["Id"] != image:
        raise RuntimeError("immutable image identity mismatch")
    (out / "image.json").write_text(json.dumps(metadata, indent=2))
    # Only package installation uses bridge connectivity. No ports or mounts.
    raw_id = call(["docker", "create", "--init", "--name", name, "--label", label,
                   "--user", "0:0", "--network", "bridge", "--cpus", "2", "--memory", "512m",
                   "--pids-limit", "96", "--entrypoint", "/bin/sh", image,
                   "-c", "exec sleep 360"]).stdout.decode().strip()
    if not re.fullmatch(r"[0-9a-f]{64}", raw_id):
        raise RuntimeError("create did not return an exact container ID")
    container_id = raw_id
    result["container_id"] = container_id
    created = owned_metadata(container_id)
    if created["Image"] != image:
        raise RuntimeError("created container uses another image")
    call(["docker", "start", container_id])
    call(["docker", "exec", "-i", container_id, "/bin/sh", "-c",
          "cat > /tmp/032-qualification.sh"], script.read_bytes())
    call(["docker", "exec", container_id, "/bin/sh", "/tmp/032-qualification.sh", "--prepare"], log="setup.log")
    call(["docker", "network", "disconnect", "bridge", container_id])
    isolated = owned_metadata(container_id)
    (out / "container-isolated.json").write_text(json.dumps(isolated, indent=2))
    if isolated["Image"] != image or isolated["NetworkSettings"]["Networks"] or isolated["Mounts"] or not isolated["HostConfig"]["Init"]:
        raise RuntimeError("container image, isolation or init contract changed")
    result["image_runtime_id"] = isolated["Image"]
    call(["docker", "exec", container_id, "/bin/sh", "/tmp/032-qualification.sh", "--inside"], log="qualification.log")
    report = (out / "qualification.log").read_text()
    expected = {mode + "-" + outcome for mode in ("baseline", "zstd1") for outcome in ("clean", "TERM", "KILL")}
    observed = re.findall(r"^CASE_PASS (\S+)$", report, re.M)
    if len(observed) != 6 or set(observed) != expected or "QUALIFICATION_PASS cases=6" not in report:
        raise RuntimeError("six distinct case proofs missing")
    result["qualified"] = True
except Exception as exc:
    result["error"] = repr(exc)
finally:
    # Repeated TERM/INT cannot bypass or restart this one 50-second cleanup.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    cleanup_started = time.monotonic()
    cleanup_deadline = min(started + 300, cleanup_started + 50)
    cleanup_errors = []
    removed_id = None
    try:
        # Discover even a create accepted before its response/ID was lost.
        ids = call(["docker", "ps", "-aq", "--no-trunc", "--filter", "label=" + label], limit=5).stdout.decode().split()
        (out / "cleanup-before.json").write_text(json.dumps({"ids": ids, "expected_id": container_id}))
        if len(ids) > 1 or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in ids):
            raise RuntimeError("ambiguous qualification ownership inventory")
        if ids:
            identity = ids[0]
            if container_id is not None and identity != container_id:
                raise RuntimeError("ownership label points to a different container ID")
            current = owned_metadata(identity)
            (out / "container-before-removal.json").write_text(json.dumps(current, indent=2))
            # Preserve bounded failure evidence BEFORE rm, not just docker exec's stdout.
            for command, filename, seconds in (
                (["docker", "logs", "--tail", "150", identity], "container.log", 4),
                (["docker", "exec", identity, "/bin/sh", "-c", """
set -eu
root=/tmp/shiftpv-032-final
for file in "$root/version.txt" "$root"/*.sha256 "$root"/*.ldd "$root"/*/*/*.log; do
  [ -f "$file" ] || continue
  printf '\nFILE %s\n' "$file"
  tail -c 8192 "$file"
done
printf '\nPROC_UPTIME\n'
cat /proc/uptime
printf '\nPROC_STAT\n'
for file in /proc/[0-9]*/stat; do
  [ -r "$file" ] || continue
  cat "$file" 2>/dev/null || :
done
"""], "container-proof-tails.log", 8),
            ):
                try:
                    call(command, log=filename, limit=seconds)
                except Exception as exc:
                    cleanup_errors.append(filename + ": " + repr(exc))
            # Recheck ownership immediately before the destructive action.
            owned_metadata(identity, limit=3)
            call(["docker", "rm", "-f", identity], log="remove.log", limit=12)
            removed_id = identity
            result["removed_id"] = identity
        # Successful inventories, not an inspect error, prove cleanup absence.
        by_label = call(["docker", "ps", "-aq", "--no-trunc", "--filter", "label=" + label], limit=5).stdout.decode().split()
        by_id = []
        exact_id = removed_id or container_id
        if exact_id:
            by_id = call(["docker", "ps", "-aq", "--no-trunc", "--filter", "id=" + exact_id], limit=5).stdout.decode().split()
        result["container_absent"] = not by_label and not by_id
        (out / "cleanup-absence.json").write_text(json.dumps({"label_query_succeeded": True,
            "label_ids": by_label, "exact_id": exact_id, "id_query_succeeded": bool(exact_id),
            "id_matches": by_id, "absent": result["container_absent"]}, indent=2))
    except Exception as exc:
        cleanup_errors.append(repr(exc))
        result["container_absent"] = None
    if cleanup_errors:
        result["cleanup_errors"] = cleanup_errors
    result["interrupted_signal"] = interrupted
    result["run_seconds"] = round(cleanup_started - started, 3)
    result["cleanup_seconds"] = round(time.monotonic() - cleanup_started, 3)
    result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    print("EVIDENCE_DIR " + str(out))
passed = result["qualified"] and result["container_absent"] is True and not result.get("cleanup_errors") and not result.get("error")
sys.exit(128 + interrupted if interrupted else (0 if passed else 1))
PY
