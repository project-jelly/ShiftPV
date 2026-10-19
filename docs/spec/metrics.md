# Metrics Contract

Metrics는 `ShiftPVPool`, `ShiftPVVolume`, `ShiftPVMove`와 node-local observation의 수렴 상태를 보여 주는
관찰면이다. Controller, Node 또는 운영 절차는 Prometheus 값을 allocation, publication, promotion, deletion,
cleanup 완료나 capacity release의 권한으로 사용하지 않는다.

## Collection contract

- Exporter는 API와 node probe의 완료된 snapshot만 게시하며 storage state를 변경하지 않는다.
- scrape 실패와 exporter 재시작은 durable API journal이나 capacity hold에 영향을 주지 않는다.
- deletion closure의 safety 기준은 wall clock이 아니라 cleanup journal이 요청한 `scanEpoch` generation과
  Pool `status.observedGeneration`의 일치다. Pool readiness freshness는 운영 관찰 evidence다.
- timestamp와 duration은 지연 감지와 운영 SLO에만 사용한다. 시간이 지났다는 이유로 hold나 data를
  해제하지 않는다.
- 식별자와 상세 오류는 CR status, Event와 log에서 찾는다.

## Emitted signals

아래 이름과 label은 현재 구현의 public metric surface다.

| Metric | Labels | Meaning |
|---|---|---|
| `shiftpv_pool_capacity_limit_bytes` | `pool`, `node` | Pool의 논리 capacity limit |
| `shiftpv_pool_reserved_bytes` | `pool`, `node` | 정확한 Pool UID에 속한 Volume owner와 미정산 Move hold의 합계; disk usage가 아님 |
| `shiftpv_pool_accounting_valid` | `pool`, `node` | 최신 hold 계산이 유효한지 여부 |
| `shiftpv_pool_ready` | `pool`, `node` | generation과 probe freshness를 포함한 Pool readiness |
| `shiftpv_pool_filesystem_size_bytes` | `pool`, `node` | 등록 directory가 속한 filesystem 전체 크기 |
| `shiftpv_pool_filesystem_available_bytes` | `pool`, `node` | `statfs`가 보고한 비특권 사용자 가용 bytes |
| `shiftpv_pool_filesystem_available_inodes` | `pool`, `node` | `statfs`가 보고한 가용 inode |
| `shiftpv_pool_inventory_valid` | `pool`, `node` | 최신 bounded inventory의 validity |
| `shiftpv_pool_inventory_truncated` | `pool`, `node` | inventory가 최대 항목 수를 초과했는지 여부 |
| `shiftpv_metrics_snapshot_success` | `source` | `metadata`, `filesystem`, `discovery` snapshot의 최근 성공 여부 |
| `shiftpv_metrics_snapshot_last_success_timestamp_seconds` | `source` | source별 마지막 성공 snapshot 시각 |
| `shiftpv_volumes` | `phase` | 고정 enum phase별 Volume 수 |
| `shiftpv_moves` | `phase` | live Volume이 `activeMove`로 참조하는 Move와 미정산 `Completing` Move 수 |
| `shiftpv_cleanup_requests` | `state` | Volume/Move에 내장된 cleanup journal 수 |
| `shiftpv_copy_observations` | `pool`, `state` | Pool inventory copy를 API authority와 대조한 Pool별 분류 수 |
| `shiftpv_mobility_deferred_volumes` | `reason` | 마지막 완료된 cordon discovery의 보류 사유별 Volume 수 |
| `shiftpv_persistent_volumes` | `phase`, `pool` | `csi.shiftpv.io`가 provision한 PersistentVolume 수를 PV phase와 현재 copy를 든 Pool별로 집계 |
| `shiftpv_persistent_volumes_released_bytes` | `pool` | `Released` PersistentVolume의 요청 capacity 합계 |
| `shiftpv_volumes_without_persistent_volume` | `pool` | 대응하는 driver PV가 없는 Ready 또는 보존 중인 Blocked Volume owner 수; 생성·삭제·이동 중 상태 제외 |
| `shiftpv_volumes_without_persistent_volume_reserved_bytes` | `pool` | 위 owner의 논리 예약 합계; 실제 disk usage나 폐기 권한이 아님 |
| `shiftpv_csi_requests_total` | `method`, `code` | 지원하는 CSI lifecycle RPC 완료 횟수 |
| `shiftpv_csi_request_duration_seconds` | `method` | 지원하는 CSI lifecycle RPC 처리 시간 |
| `shiftpv_provisioning_step_duration_seconds` | `step` | Controller의 admission·생성·삭제·helper 및 Node 실행 시간. 중첩 단계이므로 합산하지 않는다 |

`live_capacity_probe`는 Node 요청·응답 대기와 필요 시 helper fallback을 포함한다.
`statfs_helper`는 실제 helper 실행 시간이며, 정상적인 Node 응답에서는 증가하지 않는다.

CreateVolume의 세부 `step`은 다음과 같다. 같은 volume의 로그를 연결해 재시도를 구분한다.

| 단계 | `step` |
|---|---|
| 잠금 획득까지의 대기 | `create_volume_lock_wait`, `create_pool_lock_wait` |
| cleanup fence 확인 | `create_cleanup_fence` |
| 기존 생성 조회·재개 | `create_intent_read`, `create_resume` |
| 신규 Pool 후보 조회 | `create_pool_list` |
| 예약 합계와 API 목록 조회 | `create_capacity_ledger`, `create_volume_list`, `create_move_list` |
| 실제 filesystem 용량 확인 | `create_filesystem_capacity` |
| 신규 생성 intent 기록 | `create_intent_record` |
| 디렉터리 실행·완료 기록·helper 정리 | `create_directory`, `create_complete`, `create_finalize` |
| 응답 topology 조회·검증 | `create_topology` |

`capacity_admission`은 기존 intent 재개 또는 신규 admission 전체를, `create_effect`는 생성 효과와
완료·정리를 포함한다. 잠금 대기는 획득 직후 끝나며 잠금 보유 시간을 포함하지 않는다.
`create_intent_read`는 신규 생성에서 잠금 전후 두 번, 기존 intent 재시도에서 한 번 발생할 수 있다.
`create_resume`는 재개 시도 횟수이며 성공·중복 파일 생성 횟수가 아니다. 실패한 단계도 시간을 기록한다.
CSI 재시도는 정상 동작이므로 호출 횟수와 node-local effect 횟수를 구분한다.

Node metrics endpoint와 level-2 로그는 다음 실행 단계를 기록한다.

| `step` | 범위 |
|---|---|
| `node_effect_lock_wait` | RPC와 Watch가 공유하는 volume 잠금 획득 대기 |
| `node_effect_intent_read` | 잠금 획득 후 durable intent 조회 |
| `node_create_effect` | 로컬 생성부터 API 영수증 기록까지 전체 |
| `node_create_pool_root` | 생성 대상 Pool 경로 조회 |
| `node_create_authority` | 각 승인 재검증의 API 대기와 읽기 전용 backing 확인 |
| `node_create_prepare_local` | 승인 재검증 시간을 제외한 소유권 잠금과 로컬 생성 작업 |
| `node_create_receipt_local` | 승인 재검증 시간을 제외한 로컬 영수증 확인·해시 |
| `node_create_receipt_record` | 완료 영수증의 API 기록 |

실패·취소된 단계도 기록한다. 로컬 시간에는 filesystem 작업 외에 잠금과 CPU 시간이 포함되며,
`node_create_authority`는 API rate limiter 대기만의 측정값이 아니다. Watch가 RPC보다 먼저 실행할 수
있으므로 Controller의 CREATE RPC 시간과 Node의 생성 시간을 volume 로그로 함께 비교한다.
기존 완료 영수증을 재사용한 호출은 생성 단계를 다시 기록하지 않는다.

Node metrics가 켜져 있으면 API 역할(`foreground`, `observation`, `measurement`, `metadata`,
`effects`)별로 `node_api_<role>_wait`와 `node_api_<role>_roundtrip`도 기록한다.
`wait`는 토큰 예산을 기다린 시간이며 거부·취소도 포함한다. `roundtrip`은 HTTP 요청 전송부터
응답 헤더 또는 오류까지의 시간으로 body 읽기·디코딩은 포함하지 않는다. HTTP 요청을 보내지
못한 경우에도 `wait` 횟수는 증가할 수 있다. 시험 전후 histogram의 `_sum`·`_count` 차이를
비교하며, 병렬 요청의 대기 합계를 전체 작업의 경과 시간으로 해석하지 않는다. 백그라운드·재시도
요청도 같은 역할의 집계에 포함된다. 기본 로그에 throttling이 없어도 짧은 대기가 누적될 수 있다.

`pool`과 `node`는 등록된 Pool 집합으로 제한한다. PersistentVolume series의 `pool`은 volume handle로 찾은
`ShiftPVVolume`의 현재 copy가 속한 Pool이며, 대응하는 Volume이 없으면 `unknown`으로 접는다. Phase, state, source, method, code와 reason은 코드에
고정된 집합 밖의 값을 `Unknown`으로 접는다. Volume UID, Move UID, copy ID, operation/executor ID,
filesystem path, Pod UID, 오류 문자열과 timestamp를 label로 사용하지 않는다.

PV 부재 지표는 성공한 PV 전체 목록과 정확한 current copy·Volume UID·owner를 대조한다. Pool UID가
현재 등록과 맞지 않으면 `pool="unknown"`으로 집계한다. 목록 실패는 snapshot 실패로 노출하며
새로운 부재로 판정하지 않는다. `Ready` 저장과 PV 게시 사이에도 잠깐 집계될 수 있으므로 이 지표만으로
PV 삭제나 orphan을 확정하지 않는다. 외부 API나 filesystem을 변경하지 않는다.

같은 node에 여러 Pool이 있더라도 예약은 각 copy의 Pool UID로 계산한다. Pool group은
이 합계를 묶지 않는다. owner가 destination Pool로 바뀐 뒤에도 미정산 source copy의
예약은 원래 Pool에 남으며 cleanup이 정산되면 해제한다.

## Interpretation

| 관측 | 운영 의미 |
|---|---|
| `pool_ready=0` | generation, probe condition 또는 wall-clock freshness 중 하나 이상이 admission-ready가 아님 |
| inventory invalid/truncated | scan 실패, identity 문제 또는 bounded scan 전체를 증명하지 못함 |
| reserved bytes 증가 | current owner hold 또는 미정산 destination/retained-source Move hold가 증가함 |
| cleanup `Pending`/`Running` | exact intent가 filesystem effect 또는 API receipt를 기다림 |
| cleanup `Verifying`/`ConfirmingAbsence` | receipt 확인 또는 post-receipt generation absence를 기다림 |
| cleanup `NeedsReview` | parent-owned cleanup 모순으로 자동 destructive action이 닫힘 |
| copy `OrphanPreserved` | inventory에는 있지만 현재 API transaction이 소유하지 않아 report-only로 보존함 |
| PV 없는 owner | PV 게시 지연 또는 PV 없이 보존된 데이터·예약을 조사해야 함; 자동 삭제 대상이 아님 |
| Move/Volume `Blocked` | API phase상 자동 진행이 닫혀 운영자 판단이 필요함 |

API receipt만 있고 absence가 없거나 absence만 있고 receipt가 없는 상태는 정상적인 pending으로 계속
노출한다. 한 Move 동안 source와 destination hold가 함께 보이는 것은 보수적 double accounting이며 곧바로
누수로 판정하지 않는다. 반대로 physical copy 가능성이 있는데 hold가 0인 상태는 safety violation이다.

## Alerts and dashboards

기본 dashboard는 collection 상태, Pool readiness/inventory, Pool별 aggregate hold와 filesystem 여유,
Volume/active Move phase, mobility deferral, CSI rate/error/latency를 분리해 보여 준다. 기본 alert rules는
snapshot 실패·staleness, invalid Pool accounting, invalid/truncated inventory, cleanup `NeedsReview`,
orphan/missing/unsafe copy observation, filesystem 공간 소진, PV 없는 보존 owner, `Released` PV,
active Move의 `Blocked`, Pool NotReady와 지속적인 CSI 서버 오류를 경고한다. Invalid/truncated/missing inventory는 inventory 경보로 다루며
가짜 `copy_observations{state="NeedsReview"}` 수를 더하지 않는다. 실제 관측된 unsafe copy는 계속 집계한다.

생성·이동·정리 중 승인된 사본의 부재는 `InFlight` 또는 `CleanupTarget`으로 분류한다.
`Missing`은 실제 부재 관측 외에, 최신 generation의 fresh·complete inventory에 없는 현재 owner도
집계한다. 초기 생성 receipt보다 오래된 scan이나 불완전한 inventory로 부재를 추론하지 않는다.
이 지표는 비원자적으로 읽은 운영 관측이며 삭제·소유권 변경의 증거가 아니다.

현재 metric에는 개별 hold owner/role, cleanup reason class, journal별 age가 label로 노출되지 않는다.
상세 원인은 해당 Volume/Move status, Event와 log에서 확인한다.

경보의 지속 시간은 paging noise를 줄이는 관찰 조건일 뿐 state transition이나 GC timer가 아니다. Alert가
해제되어도 API receipt와 fresh absence proof가 없으면 cleanup과 capacity release는 완료되지 않는다.

영구 node/disk 손실은 자동 정상화 metric으로 감추지 않고 `Blocked`/cleanup `NeedsReview`와 보존된 hold로 계속 노출한다.
HA/replication, RWX, snapshot과 hard-quota 사용량은 0.4 metrics 계약 범위 밖이다.

### ShiftPVVolumesWithoutPersistentVolume

Pool별 PV 부재 count가 6시간 양수로 유지되고 metadata snapshot이 성공·신선할 때 경고한다. 먼저 Volume ID와 driver PV의
`volumeHandle`, provisioning 진행 상태, current copy·owner와 보존 의도를 대조한다. 삭제된 Retain PV라면
[Retain lifecycle](storage-class.md#retain-lifecycle)의 지원 경로로 같은 handle을 다시 연결하거나 명시적으로
폐기한다. 이 경보를 근거로 finalizer·Volume·디렉터리를 강제 삭제하거나 예약을 반환하지 않는다.
PV를 복구하면 부재 지표는 0으로 돌아가며 기존 예약은 유지된다.
이 경보와 `ShiftPVReleasedVolumes`는 Pod·instance label을 집계에서 제거하므로 관측이 이어지는
exporter Pod 교체로 6시간 대기를 다시 시작하지 않는다. 관측 자체가 끊기면 별도의 stale 경보를 확인한다.

### ShiftPVPoolNoSpace

성공·신선한 filesystem 관측에서 가용 bytes 또는 inode가 0인 상태가 5분 유지되면 경고한다.
`Writable=True`는 작은 write/fsync probe의 성공이며 새 파일이나 cleanup metadata 공간을 보장하지 않는다.
먼저 filesystem의 공간·inode와 외부 writer를 확인하고 안전하게 식별한 외부 점유를 줄인다. cleanup은
durable intent를 기록하지 못하면 계속 대기한다. 데이터·metadata를 임의 삭제하거나 receipt·absence
검증을 건너뛰지 않는다. 여유 회복 후 새 관측과 정상 재시도로 수렴하는지 확인한다.

### ShiftPVMoveBlocked

성공·신선한 metadata 관측에서 active Move의 `Blocked` 수가 5분 유지되면 경고한다.
Move status의 reason, current owner, publication과 cleanup 증거를 대조하고 지원되는 recovery를 선택한다.
보존된 과거 terminal Move는 이 지표에 포함하지 않는다.

### ShiftPVPoolNotReady

성공·신선한 metadata 관측에서 Pool NotReady가 5분 유지되면 경고한다. Pool conditions의 generation,
마운트·backing identity, 쓰기 가능 여부와 peer capacity 증거를 확인한다. 원래 마운트가 유실됐다면
원래 filesystem을 복원한다. 빈 디렉터리나 다른 디스크를 같은 경로에 놓아 복구로 취급하지 않는다.

### ShiftPVCSIErrorRate

RPC method·component·node별 5분 요청 중 서버·인증 오류 비율이 5%를 넘는 상태가 5분 유지되면 경고한다.
대상 code는 `Unknown`, `DeadlineExceeded`, `PermissionDenied`, `Internal`, `Unavailable`, `DataLoss`,
`Unauthenticated`다. 정상적인 capacity 대기(`ResourceExhausted`), precondition과 경쟁 재시도는 제외한다.
API 가용성, 인증·권한, 해당 CSI code와 로그를 확인한다. exporter Pod별 counter에 `rate`를 적용한 뒤
합산하므로 counter reset을 오류 증가로 세지 않는다.
