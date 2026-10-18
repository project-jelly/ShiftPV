# Testing

테스트의 목적은 “한 번 성공했다”가 아니라 0.4 계약이 경쟁, 응답 유실, process/node 재시작 뒤에도
안전하게 수렴한다는 증거를 만드는 것이다. 모든 명령은 repository root에서 실행한다.

## Evidence ladder

| Gate | 환경 | 증명하는 범위 | 운영 승인 조건 |
|---|---|---|---|
| Design model | pure Go state graph | invariant와 stable recovery path의 내부 일관성 | 현재 사용 가능 |
| Primitive probe | ephemeral Kind/container | Kubernetes와 rsync 전제의 사용 가능성 | 현재 사용 가능 |
| Unit/race | fake API + fault injection | 실제 CRD field, CAS, journal, 멱등 action | 0.4 구현과 함께 필수 |
| Linux integration | real mount namespace/filesystem | local lock, bind mount, rename/fsync/purge | 0.4 구현과 함께 필수 |
| Isolated Kind | API server, scheduler, kubelet, CSI | end-to-end lifecycle와 node/process failure | 0.4 구현과 함께 필수 |
| Real node fault | 대상 filesystem과 process/unclean OS reboot | reboot 뒤 receipt와 directory durability | 운영 전 필수 |
| Soak | 운영 동형 다중 node | 반복 이동·삭제, leak, capacity drift, alert | 운영 전 필수 |

낮은 gate의 성공은 높은 gate를 대체하지 않는다. 특히 model, Kind, Docker overlay 결과를 실제 disk의
reboot durability로 해석하지 않는다. 물리 전원 차단은 별도 환경 qualification이며 기본 release gate가 아니다.

## Design preflight

```bash
make v04-model
make v04-kubernetes-primitives
make v04-filesystem-primitives
```

`v04-model`은 source publish race, partial copy, owner commit 전후 실패, unlink 직후 receipt 기록 전
재시작, stale/invalid/incomplete observation, identity contradiction, capacity hold를 상태 그래프로 탐색한다.
두 node가 다시 사용 가능하고 증거가 모순되지 않으면 terminal state로 수렴해야 한다. 모순은 자동
삭제 대신 review 상태로 격리한다.

Kubernetes probe는 다음 전제를 실제 API server에서 확인한다.

- status write는 `metadata.generation`을 올리지 않는다.
- `spec.scanEpoch` 변경은 generation을 올린다.
- deletionTimestamp 뒤에도 finalizer가 parent와 journal을 보존한다.
- terminating parent의 status journal을 갱신할 수 있다.
- owner-referenced suspended Job은 parent finalizer가 있는 동안 유지되고 parent 삭제 뒤 정리된다.
- `spec.initialPool`은 Create·status 쓰기에서 보존되고, 구버전 객체에 최초 추가만 허용하며 변경·제거는 거부한다.
- gated Pod의 한 번의 Update에서 owner selector 추가와 ShiftPV gate 제거가 가능하며, 같은 UID와 사용자 gate·selector·affinity가 보존된다. API 제약 검증이며 controller 전체 E2E는 아니다.

Filesystem probe는 immutable helper image에서 baseline과 copy-only zstd level 1을 각각
clean/TERM/KILL로 검증한다. SHA256·checksum dry-run, hardlink·symlink·FIFO·UID/GID·mode·sparse·ACL·xattr를
확인하고 실제 partial write 뒤 기존 process group의 종료를 확인한 후 같은 destination으로 재시도한다.
Copy의 fsync와 검증 command는 유지한다. 도구 설치 전후 원래 rsync와 라이브러리 hash가 같아야 한다.
Controller image CI가 이 검사를 실행하며 `.tmp/rsync-qualification/`을 artifact로 보관한다.
검사 도구 설치에만 network를 사용한다. Device node·nested filesystem, 운영 성능과 power-loss 검증은 별도다.

## Fast repository gate

```bash
make verify
go test -race -count=1 ./...
git diff --check
```

`make verify`는 model과 repository-local gate를 함께 실행한다. Docker/Kind probe는 별도 gate이며
관련 CI에서 실행한다. Copy option·base image·Kubernetes version·CRD protocol 전제가 바뀌면 해당
probe를 다시 검증한다.

## Required protocol tests

`CAPACITY_TRACKING_ONLY=1 ./test/e2e/kind/run.sh`는 실제 scheduler에서 꽉 찬 선호 노드 우회,
고정 노드의 공간 반환 후 진행, 다른 노드에만 있는 그룹 배치, 게시 재시작·0 반환·롤백과
uninstall 후 용량 객체 GC를 검증한다. `get_capacity_test.go`는 여러 Pool의 합계와 단일 볼륨
최대값, 초기 생성·이동 hold, 관측 누락·오류 및 조회의 무변경 계약을 DI로 검사한다.

| Contract | 최소 fault boundary |
|---|---|
| Node creation | copy intent·stage mkdir·placement marker·rename·API receipt 경계의 중단 상태를 실제 Pool reconcile·승인 검사에 연결; inventory 수동 보정 없이 동일 작업으로 복구 |
| Original creation claim | DI로 live PVC UID·선택 노드·API 오류 검증; 첫 intent 전 영속화, 응답 유실 뒤 재시도, 같은 이름의 새 UID 거절, Ready replay·기존 정보 없음 보존. API primitives에서 CRD pruning 회귀·변경/삭제/뒤늦은 추가 거부 및 정상 취소 확인 |
| Automatic creation retirement | 실제 Registry/cleanup store로 완료 CAS 승자·취소 응답 유실·재시작·외부 finalizer·정산 전후 예약 확인. PVC 조회 오류·PV 인계·Ready/출처 불명 보존, 막힌 PVC와 독립 작업 진행 검증. API primitives는 같은 이름의 새 UID 뒤 spec-only 자동 정리를, Kind node-rpc는 완료 영수증 차단을 유지한 채 실제 부분 생성 디렉터리 정리를 검증 |
| Owner commit | CAS 수락 전/후, 응답 유실, stale currentCopy/owner 관찰, 동시 delete/move |
| Publication | NodePublish lock 획득 전/후, publishedNodes와 actual mount 관찰 차이, kubelet target 잔존, 재시작 |
| Copy | partial write, ENOSPC, read-only, checksum mismatch, Job 교체, copy receipt 응답 유실 |
| Promotion | rename 전/후, file/tree fsync 실패, marker mismatch, process kill |
| Source cleanup | intent 전/후, unlink 전/후, local/API receipt 전/후, absence scan 전/후 |
| Volume delete | mounted target, node down, already-absent-with-intent, unexpected absence, finalizer race |
| Workload retirement | original UID absent/replaced versus API error/in-use deletion; pre/postcommit; CAS race, response loss, restart, retained bytes; CSI deletion after unlock |
| Capacity | destination reserve, commit 직후 두 copy, 각 receipt 단독, fresh absence, abort cleanup |
| Measurement | 측정 전후 Pool/mount/copy 변경, API·du 실패, read-only bind, mount 유실 |
| Inventory | generation stale, invalid signature/identity, incomplete/truncated scan, copy reappearance, 물리 수집 전후 생성 완료, stage 이동·경로 타입 변경·parent symlink, 경로 변경 중 부재 승인 거부 |
| Removal | unresolved Volume/Move/hold, unavailable node, API read error, generation-fenced empty inventory |

각 fault case는 반복 reconcile 뒤 다음을 함께 확인한다.

1. current owner identity와 authoritative bytes
2. source/destination 물리 경로와 mount 상태
3. Volume/Move phase, journal, finalizer와 executor Pod/Job UID
4. Pool observed generation, validity, completeness와 inventory
5. source/destination capacity hold 합계
6. restart count, warning event, bounded-cardinality metric
7. terminal state 이후 남은 resource와 test fixture cleanup

## Four review criteria

Lifecycle closure work follows the [stage/event contracts](lifecycle-closure-plan.md).
Mobility test clients assign a fresh UID to each created instance. The g3 Kind
scenario deletes an approved placement while the controller is stopped, observes
the new API UID, and requires the original PVC/checksum and full Move settlement.
It also holds the exact Volume's owner commit while recreating the consumer in
Promoting, then requires the new held Pod to publish after commit. Reconciler
tests cover recreation during destination publication wait and source cleanup,
changed binding/owner/Pod UID on retry, and a stored release with a lost response.

테스트 파일은 컴포넌트별로 유지한다. 각 변경은 다음 네 기준에서 검토하며, DI는 관찰·시간·요청
예산의 실패를 주입한다. 실제 판정·Registry·lock을 연결하는 회귀 테스트도 함께 유지한다.

| 대상 | 안정성 | 진행성 | 동시성 | 기록 수명 |
|---|---|---|---|---|
| 삭제 | identity/Move 모순이면 거부 | 정상 publication은 대기 후 재개 | CAS 충돌 뒤 새 상태로 재판정 | receipt와 fresh absence 전에는 finalizer/hold 유지 |
| Pool 배치 | capacity 초과 승인 금지 | 독립 Pool의 요청은 진행 | 같은 Pool UID만 직렬화 | durable intent retry는 같은 copy/hold 유지 |
| Metadata GC | API 참조·불명확한 물리 증거면 보존 | fresh evidence 복구 후 수집 재개 | API 대기 중 Pool marker lock과 foreground 예산 사용 금지 | 완료·보존 기간·무참조·부재가 모두 충족된 reclaim 기록만 제거 |

| 회귀 테스트 | 실제 연결 경계 |
|---|---|
| `volume/deletion`: `TestDeletionAdmissionSafety`, `TestDeletionAdmissionProgress` | 외부 I/O 없는 판정과 상태 전이 |
| `kubernetes/volumeapi`: `TestBeginDeleteRechecksAdmissionAfterCASConflict` | 실제 Registry CAS + 충돌/동시 변경 주입 |
| `csi/controller`: `TestDeleteWaitsForPublicationWithoutStartingCleanup` | CSI 응답 + 실제 Registry + cleanup effect 호출 여부 |
| `csi/controller`: `TestConcurrentAdmissionKeepsIndependentPoolHoldsOnSameNode`, `TestConcurrentAdmissionToSamePoolCannotOversubscribe` | 실제 Pool Locker + capacity probe 지연 + durable hold |
| `csi/controller`: `TestCreateInitializationRecoversAPIWriteBoundaries`, `TestLegacyInitializationsReadmitWithoutBlockingEachOther` | 실제 Registry/ledger + Create·status 실패/응답 유실 + 재시작 + 다른 Pool 진행 |
| `volumeapi`: `TestUnstartedCancellation*`; `csi/controller`: `TestUnstartedCreationCanSettleWithoutFilesystemEffects`; API integration: `TestAPIServerUnstartedCancellation` | UID/RV 삭제·초기화 경합, 응답 유실·재시작, 외부 finalizer와 예약 보존, 실제 API에서 CSI 취소와 지연 초기화 거부 |
| `csi/controller`: `placement_progress_test.go`, `TestMissingPoolUsesPlacementAuthority` | 등록 부재·일시 NotReady·소비자 배치 권한을 구분하고 원래 생성 intent·예약을 보존 |
| `pool/readiness`: `backing_boundary_test.go`; `volumeapi`: `pool_backing_test.go` | 실제 Registry·Probe DI로 승인된 peer 유실/복구, 신규 등록 경쟁, own 실패·세대·구버전 condition 호환을 검증 |
| `node/executor`: `peer_backing_progress_linux_test.go`; `csi/node`, `node/observation`, `mobility/controller`: `backing_test.go` | peer 장애 중 기존 작업·실제 create/purge 진행, 실행 전후 own identity 변경 거절, receipt 이후 fresh absence 전 hold 유지 |
| `mobility/controller`: `preparation_boundary_test.go`, `prepared_execution_test.go`, `prepared_cancellation_test.go` | 실제 Registry/RV CAS + 5Gi에 2Gi 4개 요청, hold 전환·응답 유실·지연 claim/Moving PUT·binding 부재·placement NotFound와 취소 정산 |
| `e2e/kind/mobility/preparation.sh` (preparation) | 실제 scheduler/API admission + 2개 hold/4개 원본 유지, restart, 2개 완료/2개 원본 유지 |
| `e2e/kind/mobility/legacy-upgrade.sh` (preparation) | candidate preflight의 active legacy 거절 → old writer/helper image로 정산 → 교체 전 complete·blocker 없음 확인 → Recreate 후 같은 PVC/PV/Volume/Move UID·데이터·history 보존 및 새 legacy actor 미생성 |
| `e2e/kind/mobility-node-restarts.sh` (source/destination) | 실행 승인 전 노드 장애 → 취소·같은 held Pod 복구; test-owned placement finalizer로 삭제 대기 중 hold 유지와 NotFound 후 정산을 검증. source에 쓴 PVC별 고유 데이터로 재생성 없는 보존 확인 |
| `mobility/controller`: `approval_recovery_test.go` | 승인 직후 source 장애·원자적 copy 계획·재시작·fresh absence 전 hold 유지; 계획 없는 승인은 추론·scan 없이 보존 |
| `mobility/history`: `classify_test.go`; `lifecycle/upgrade`: `checker_test.go` | cleanup 없는 prepared 복구의 exact CAS 정산을 읽기 전용으로 승인; legacy·불완전 계획·미해제 hold·cleanup 증거 누락·실행 중 helper는 거절 |
| `node/executor`: `move_cleanup*_linux_test.go`; `provisioning/nodeexecutor`: Move cleanup tests | source/incoming/serving 실제 purge·동일 Volume RPC/watch 직렬화·권한 변경·receipt 응답 유실·Pod 교체·구버전 capability와 Job binding 경쟁 |
| `mobility/controller`: `native_cleanup_test.go`, `cleanup_job_reentry_test.go`; `nodeexecutor`: `resident_cleanup_test.go` | Node receipt 이후 fresh absence 전 hold 유지; 미정산 legacy Job은 호환성 blocker로 보존하고 실행·전환하지 않음 |
| `csi/controller`: `cleanup_job_reentry_test.go` | 실제 Registry·Store·Client와 명시적 과거 snapshot으로 미정산 Job·잔존 Pod·ConfirmingAbsence fence 보존, 이전 writer의 정산 증거가 있어야 삭제 완료 |
| `helperpod`: `inspector_test.go`, `inspector_move_test.go`; `nodeexecutor`: `cleanup_advance_test.go` | GET/LIST만으로 exact legacy history·Job/Pod 종료·orphan owner를 확인; 여러 Move에 LIST 2회, 불완전 관찰은 보존 |
| `mobility/controller`: `cleanup_pending_test.go`, `cleanup_dispatcher_test.go` | Node cleanup 대기 3개가 worker 2개를 점유하지 않고 독립 Move 계획 저장 |
| `mobility/controller`: `source_usage_boundary_test.go`; `helperpod`: `source_usage_test.go`; `volumeapi`: `move_usage_test.go` | 실제 Registry·helper Pod 경로의 응답 유실·재시작·취소·terminal 잔여 Pod와 3개 측정 대기 중 독립 Volume 진행; 결과 CAS와 양수 사용량 불변성 |
| `mobility/controller`: `recovery_response_loss_test.go` | 실제 Registry의 저장 후 EOF/API timeout과 저장 전 실패에서 진단 재쓰기 금지, 원래 오류 보존, fresh read 뒤 복구 재개 |
| `cmd/controller`: `mobility_probe_test.go`; `pool/measurement` clients; `mobility/controller`: capacity probe tests | 실제 HTTP 지연을 포함한 resident 조회 전체 시간 예산, helper fallback 부재, fresh nonce, 중단 전 확인·pending 시 예약과 source 권한 유지 |
| `mobility/controller`: `volume_budget*_test.go`, `volume_expiry_boundary_test.go`, `expired_status_test.go` | 실제 HTTP 지연이 worker 수보다 많아도 독립 Volume 진행, 동일 Volume 직렬화, 상위 deadline 상속, 만료 후 다음 작업·진단 중단; 응답 유실로 저장된 상태는 보존하고 fresh read로 재개 |
| `volume-helper`: measurement tests; `pool/capacity`: measurement tests | scan generation 변경 중 du 1회 유지·최신 inventory/실제 backing 재확인·명시적 실패와 config/copy/publication 변경 시 결과 폐기 |
| `e2e/kind/mobility/native-cleanup.sh` (node-cleanup) | 실제 API receipt 거절·commit 이후 drain·Node/controller 교체 뒤 같은 operation과 local receipt 재사용, fresh absence 후 완료·PVC/PV/checksum 보존 |
| `mobility/controller`: `owner_placement_test.go`, `owner_placement_boundary_test.go` | 실제 admission patch → Move 생성 전 uncordon·controller 재시작 → ReplicaSet/VMI owner의 같은 Pod gate 회수; ownerRef·사용자 제약 보존, 충돌 뒤 UID/binding 재검증. 실제 VM 실행 시험은 아님 |
| `mobility/controller`: `dispatcher_boundary_test.go`, `dispatch_scan_test.go`, `discovery_observations_test.go` | 실제 Registry/RV CAS + A의 API 대기 중 B 진행·동일 Volume 직렬화·재시작 relist; stale UID/잘못된 lane 거절·terminal GC·통계 합산 |
| `pool/capacity`: `locker_test.go`; `mobility/controller`: `pool_busy_test.go`, `ledger_handoff_test.go` | 공유 Lock/TryLock 상호배제, busy 중 placement/hold 보존, fresh retry·owner CAS·용량 원장 경계 |
| `node/executor`: `watch_progress_linux_test.go` | 실제 Node watch·파일 효과 + A cleanup 대기 중 독립 Pool B 생성, A 재개 뒤 exact receipt 정산 |
| `e2e/kind/mobility/preflight.sh` (g1) | 격리된 workload selector로 early drain → held replacement → uncordon 후 같은 Pod UID Ready·Move 미생성; 정상 이동·cleanup·activeMove 정산 후 빈 source 대상 drain → destination Pod/PVC/PV/Volume/Move identity와 고유 checksum 보존 |
| `node/ownership`: `pool_release_test.go` | 실제 marker lock + 중단된 임시 파일 회수 + symlink/hardlink/권한/소유자 불일치 보존 |
| `cmd/node`: `TestNodeBackgroundBudgetDoesNotBlockForegroundRequests` | 실제 Collector/Registry/REST clients + 차단된 GC limiter + HTTP 서버 |
| `node/metadata`: `TestPoolCollectionEligibility`, `TestCollectorProgressAfterFreshEvidence`, `TestCollectorRequiresLiveUnreferencedReadyPool` | 순수 eligibility + 새 관찰/참조/오류 주입 + effect 진입 여부 |
| `node/ownership`: `TestMetadataGCRequiresClosedExpiredExactRecord`, `TestMetadataGCRechecksPhysicalAbsenceAfterAuthority`, `TestMetadataGCDoesNotHoldPoolMarkerLockDuringAPIRead`, `TestMetadataGCAfterPoolReregistration`, `TestPoolReleasePreservesLockedNamespaceAndThenRemovesLocks` | 실제 파일·receipt·flock + 보존 시간/재등장/등록 교체/중단 상태 |

GC HTTP 테스트는 공유 limiter로 인한 정체를 검증한다. API 서버 장애와 실제 publish/클러스터
지연은 별도 gate다. 중단 상태 fixture는 process kill이나 전원 차단의 내구성을 증명하지 않는다.

복구 테스트의 용량 검증도 운영과 같은 `ReservedBytesForPool`을 사용한다. Owner commit 전후,
cleanup 미완료·완료, 같은 node의 서로 다른 Pool hold와 overflow를 보존한다.
`BenchmarkReservedBytesForPool`은 Pool ledger의 CPU·할당 비용만 측정하며 API·disk 지연을 포함하지 않는다.

## Isolated Kind

Target suite는 서로 다른 host directory를 mount한 최소 두 worker를 사용한다. Suite마다 고유한
`CLUSTER_NAME`과 kubeconfig를 사용하고 성공·실패 모두 자신이 만든 cluster와 directory만 정리한다.

```text
control-plane
├── worker-a ── Pool A
└── worker-b ── Pool B
```

필수 시나리오는 provision/publish, cold move 양방향, 각 phase의 Controller 재시작, source/destination
node stop/start, Volume 삭제, Pool 삭제, uninstall 차단과 재시도다. 테스트가 status를 직접 성공으로
patch하거나 host-side copy로 제품 effect를 대신하면 안 된다.

전체 suite와 독립적으로 재현 가능한 lifecycle gate는 다음과 같이 실행한다.

```bash
NODE_EFFECTS_ONLY=1 CLUSTER_NAME=shiftpv-node-focused ./test/e2e/kind/run.sh
VOLUME_DELETE_CLEANUP_ONLY=1 CLUSTER_NAME=shiftpv-delete-focused ./test/e2e/kind/run.sh
CLEANUP_EXECUTOR_RETRY_ONLY=1 CLUSTER_NAME=shiftpv-cleanup-retry-focused ./test/e2e/kind/run.sh
MOBILITY_NODE_RESTARTS_ONLY=1 CLUSTER_NAME=shiftpv-mobility-restart-focused ./test/e2e/kind/run.sh
```

Node gate는 상주 Node 생성 receipt, Pod 교체 뒤 데이터 보존, 새 Pod UID의 purge receipt와 fenced cleanup 완료를 확인한다.
Volume delete gate는 cleanup 완료 전 Volume finalizer와 capacity hold가 유지되고, generation-fenced absence 뒤에만
삭제와 용량 재사용이 일어나는지 검증한다. Cleanup executor retry gate는 receipt 전 resident Node Pod 교체에서
같은 operation·target을 보존하고 새 exact Pod UID로 재지정해 수렴하며 cleanup Job이 생기지 않는지 확인한다.
Mobility restart gate는 owner commit 전후 및
`CleaningSource`에서 source/destination node가 중단됐다가 돌아온 뒤 현재 transaction이 계속 수렴하는지
검증한다.

## Multi-Pool qualification

실제 ext4/xfs의 thick LVM 또는 독립 disk에서 다음을 확인한다. 합성 backing, tmpfs와
단일 Pool Kind 결과는 이 검증을 대신하지 않는다.

1. 같은 node의 독립 Pool 두 개가 Ready가 되고 그룹별 PVC가 각 경로에 생성된다.
2. 같은 그룹에서 한 Pool의 여유가 부족하면 다른 Pool을 선택한다.
3. 공유 filesystem·겹치는 backing·thin 구성은 쓰기 probe와 inventory 전에 거부한다.
4. mount 유실·교체와 Pool 재생성 시 새 배치·게시가 거부된다.
5. 삭제·이동 hold가 해당 Pool UID에 남고 기존 단일 directory Pool도 동작한다.
6. 승인된 B의 mount 유실 중 정상 A의 기존 copy 게시·승인된 실행·정리는 진행하고 새 할당은 보류한다.
   B 복구 후 신규 할당이 재개된다. DI 회귀시험은 실제 LVM·disk의 이 검증을 대신하지 않는다.

## Real-node qualification

[Full filesystem qualification](../../test/integration/full-filesystem/README.md)은 격리된 Linux ext4에서
block·inode를 각각 소진하고 readiness와 cleanup의 `NoSpace`, fixture 공간 반환 후 복구를 확인한다.
전체 cleanup transaction이나 full filesystem에서의 자동 공간 확보를 증명하지 않는다.

운영과 같은 filesystem 및 mount option에서 다음을 수행한다.

- copy와 promotion 각 crash point에서 helper `SIGKILL` 및 node reboot
- source/destination 중 한 node를 장시간 차단한 뒤 같은 transaction 재개
- destination publish 직후 reboot 뒤 source 보존 여부 확인
- cleanup unlink 직후 reboot 뒤 receipt/absence 재구성 확인
- disk full, inode full, read-only remount, 느린 I/O와 API outage 조합

합격은 checksum만으로 판정하지 않는다. Metadata, identity marker, fsync/rename 결과, Kubernetes journal,
capacity accounting이 같은 결론을 가리켜야 한다.

## Soak exit criteria

Real-node release gate는 운영 동형 환경에서 **성공한 Move 100회와 경과 시간 43,200초를 모두**
충족해야 한다. 10회마다 controller를 재시작하고, 20회마다 native cleanup의 bound Node Pod를
교체하며 해당 fault의 실제 성공 횟수도 확인한다. 간격 0은 smoke 전용이며 native cleanup 간격
1은 이전 settled Move가 없어 거절한다. 짧은 smoke run이나 mock 검증은 실제 노드 qualification을
대신하지 않는다. PVC/PV/ShiftPVVolume UID와 PV name/CSI `volumeHandle`을 정확히 보존하고,
초기 payload에 unique `RUN_ID`/`PVC_UID`/`VOLUME_UID` nonce를 넣어 재생성을 검출한다.

Native fault는 cordon 전에 동일 test Volume의 이전 `Succeeded`/cleanup `Completed` Move를
변경 없이 server dry-run하여 거절되는 것으로 admission barrier 활성화를 증명한다(저장 없음).
Volume barrier로 cleanup 승인을 막은 뒤 source unpublish와 destination
owner commit을 확인하고 기존 lock inode에 `flock`을 잡는다. Barrier 해제 후 journal에 bound된
정확한 Node Pod UID를 UID precondition으로 삭제한다. 같은 operation/target이 새 Pod UID로
rebind된 뒤 lock을 해제하고, native receipt의 `executorUID`가 새 Pod UID와 같은지 확인한다.
Receipt 승인과 fresh source absence 증거가 확보되기 전에는 capacity hold를 해제하지 않는다.
Context·역할별 노드/SSH 대상·immutable image 승인은 [real-node 절차](../../test/e2e/real-node/README.md)를 따른다.
성공 marker는 owned lock/policy 해제, uncordon, 필수 final API 증거 수집 뒤 출력한다.
`INT`는 130, `TERM`은 143으로 종료한다.

완료 시 다음이 모두 0으로 돌아와야 한다.

- non-terminal Move와 deleting Volume
- parent 없는 executor Job
- 설명되지 않는 physical copy와 capacity hold drift
- dual mount 또는 non-owner publish
- 주입하지 않은 restart 증가와 persistent `Blocked`/cleanup `NeedsReview`

실패한 run은 resource snapshot, logs, metrics, directory inventory를 보존하고 자동 재실행으로
덮지 않는다.

## CDI provisioning latency

Inject claim/Pod reads and placement inspection failures. Cover CDI import/upload
prime before Pod creation, reverse ownership, ordinary scheduler consumers,
fixed oversized requests, UID/node changes, and capacity-return convergence.
The focused Pool capacity Kind test models scratch and prime with the real
external-provisioner; full CDI DataVolume import remains a separate qualification.

CSI request regression tests reject unsupported sources, mutable parameters,
negative bounds and invalid topology before admission. Compatible retries cover
Pending/NodeCreating/Ready and retain actual capacity and copy identity; a real
Registry test resumes a saved intent through fresh controller instances.

Create observation tests separate new admission from existing-intent retries,
inject Volume/Move list failures, and block a real Pool UID lock and capacity
probe independently. Lock-wait timing must finish before protected work; failed
API stages must report duration without proceeding to intent or filesystem work.

For the two-VM/250Gi Pool scenario, compare reservation return → next
`CreateVolume` start, Node effect count, and successful creation duration.
Use Controller level-2 step logs and, when enabled,
`shiftpv_provisioning_step_duration_seconds`. Nested durations must not be summed.
Use the [CreateVolume step names](../spec/metrics.md#emitted-signals) to separate
lock wait, API lists, intent recording, node effects and topology response.
Compare repeated calls for the same volume ID; `create_resume` marks an existing
intent retry, while `create_intent_record` marks a new admission attempt.
The [direct CSI profile](../../test/measurement/provisioning/README.md) compares
new calls, completed-intent retries and concurrent callers in an isolated cluster.
Its DI regression checks CSI client budget isolation and independent Pool holds.
Confirm `shiftpv.io/capacity-retry` preserves selected-node, and repeat with a
Controller restart and a different Pool group. Check that Deleting Volumes and
unsettled Move holds remain charged. Fake API timing is not VM latency evidence.

`TestChangingInventoryRecoversWithoutExternalWake` runs the resident observer with
an injected clock: a changing inventory must recover without a manual Reconcile
or watch event, and a peer inventory must not be rescanned by the short retry.
`TestObservationRecoveryConnectsScannerToWaitingCSIClaim` connects both Run loops,
the real Registry, Pool status watch and CSI retry notification. Its inventory
failure and API server are injected; it is not a live disk or sidecar latency test.
Observation waits wake on a matching Ready Pool even when quota is full; the next
CSI call must still perform capacity admission. UID, selected-node and bound-claim
guards apply to both observation and capacity notifications.
After a selected Pool's live probe fails, an unchanged Ready snapshot must not
notify it repeatedly; require a newer successful observation or another candidate.

`TestApprovedCreationProgressesWhileSiblingScanIsPending` executes real native
creation with injected mount/sysfs evidence, retaining the exact receipt, hold
and all four live backing checks. `TestApprovedCleanupIgnoresSiblingScanButRetainsPostEffectFence`
adds real Linux purge and proves the old inventory cannot settle its post-effect
absence fence. The final complete scan is simulated; physical LVM qualification
remains separate. Helper and new-publication tests retain current-generation gates.

Unpublish regressions inject real backing probes and a sibling deletion's
`scanEpoch` change before and inside the storage lock. A 20-volume burst uses
real REST clients, preserves an unrelated published copy, and leaves the Pool
observation stale. Run `go test ./src/cmd/node -run '^TestNodeUnpublishBurst' -v`
for API counts, or `-run '^$' -bench '^BenchmarkNodeUnpublishBurst$' -benchtime=1x`
to compare the former 5/10 budget with the current configuration. Separately
stall GC and Pool observation and verify that foreground calls still progress.

The required Linux mount gate runs `TestLinuxPublicationUnpublishReadOnlyPool`
in a private mount namespace: publish two real bind targets, remount the Pool
read-only, unmount each target and reconcile publication. It must retain the
first remaining reference and clear only the last, preserving copy identity
and data. Unit tests cover exclusion by a writer, writable missing-lock
initialization, and no retry after a callback reports NotFound. The Linux
failure baseline reaches actual unmount, then fails opening the lock with EROFS.

Node API observation tests gate token waits and HTTP responses independently,
preserve cancellation and retryable budget errors, and verify typed/dynamic
clients share one observed limiter. The creation budget regression uses two
registered Pool identities and 20 volumes with real REST clients: exhausting
the shared budget must stall requests; releasing it must settle all exact
creation receipts in their own Pool roots. These are dependency-injected
contracts, not cluster latency measurements.

Controller admission has its own 50 QPS/100 burst budget shared by its typed and
dynamic readers. `webhook_budget_test.go` exercises the production constructor
and HTTP `/mutate` path while a background request remains blocked in an inherited
limiter. The request must still resolve PVC, PV, Volume and owner Node. The same
suite checks `/validate-delete` with empty/retained dependencies, API denial and
caller cancellation. Pool safety/quiesce/CSI clients cannot consume this budget;
each role retains its own bounded client. These tests establish isolation and
preserved admission policy, not production throughput or a latency percentage.

For a live throughput comparison, enable the existing Node metrics endpoint
and capture `node_api_effects_wait` and `node_api_effects_roundtrip` histogram
sum/count before and after the same 20-volume workload. Record API failures and
Node restarts too. Separate accumulated token waits from HTTP response-header
time; neither metric includes response decoding. Compare one and two Pools
with the same total requests, then vary one bounded API budget at a time.

For Node gRPC, record `live_capacity_probe` and `ShiftPV Node RPC` logs for
CAPACITY/CREATE/RECLAIM. Verify Pod replacement reconnects, concurrent RPC and
Watch recovery write one immutable receipt, and lost replies retain holds.
Node effect observation tests block authority API reads independently of the
shared gate, reject failed authority before filesystem work, and preserve the
creation receipt on retry. An injected clock verifies local durations exclude
nested authority checks, including failures and subsequent stages.
`node/executor` retains four fresh creation-authority checks, with one Pool GET
per check. Inject Pool/Pod identity, protection, path and backing changes at
each boundary; no failed check may record a creation receipt.
Mobility tests keep rejected Pool identities visible, isolate unapproved
candidates, retain approved peer conflicts, and continue persisted Moves after
discovery errors. API/decode failures must never grant placement.
Inject invalid inventory after the serving directory exists: the approved
creation must record the same operation's receipt while new placement stays
blocked. Reconcile interrupted serving stages without manually restoring inventory.
Reject wrong caller/audience, executor UID, operation ID and Pool evidence.
Repeat capacity reads with mount loss; Pool generation/scanEpoch must not change.
With RPC disabled or older Nodes, verify the API probe nonce and answer still
match and readiness updates preserve concurrently recorded answers.
Delete completion within `--cleanup-absence-wait` must release the hold in one
call; timeout/cancellation and stale inventory must keep it charged.
