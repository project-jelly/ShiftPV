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
| `shiftpv_csi_requests_total` | `method`, `code` | 지원하는 CSI lifecycle RPC 완료 횟수 |
| `shiftpv_csi_request_duration_seconds` | `method` | 지원하는 CSI lifecycle RPC 처리 시간 |
| `shiftpv_provisioning_step_duration_seconds` | `step` | Controller의 admission·생성·삭제 및 helper 처리 시간. 중첩 단계이므로 합산하지 않는다 |

`live_capacity_probe`는 Node 요청·응답 대기와 필요 시 helper fallback을 포함한다.
`statfs_helper`는 실제 helper 실행 시간이며, 정상적인 Node 응답에서는 증가하지 않는다.

`pool`과 `node`는 등록된 Pool 집합으로 제한한다. PersistentVolume series의 `pool`은 volume handle로 찾은
`ShiftPVVolume`의 현재 copy가 속한 Pool이며, 대응하는 Volume이 없으면 `unknown`으로 접는다. Phase, state, source, method, code와 reason은 코드에
고정된 집합 밖의 값을 `Unknown`으로 접는다. Volume UID, Move UID, copy ID, operation/executor ID,
filesystem path, Pod UID, 오류 문자열과 timestamp를 label로 사용하지 않는다.

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
| Move/Volume `Blocked` | API phase상 자동 진행이 닫혀 운영자 판단이 필요함 |

API receipt만 있고 absence가 없거나 absence만 있고 receipt가 없는 상태는 정상적인 pending으로 계속
노출한다. 한 Move 동안 source와 destination hold가 함께 보이는 것은 보수적 double accounting이며 곧바로
누수로 판정하지 않는다. 반대로 physical copy 가능성이 있는데 hold가 0인 상태는 safety violation이다.

## Alerts and dashboards

기본 dashboard는 collection 상태, Pool readiness/inventory, Pool별 aggregate hold와 filesystem 여유,
Volume/active Move phase, mobility deferral, CSI rate/error/latency를 분리해 보여 준다. 기본 alert rules는
snapshot 실패·staleness, invalid Pool accounting, invalid/truncated inventory, cleanup `NeedsReview`,
orphan/missing/unsafe copy observation, 그리고 회수 판단을 기다리는 `Released` PersistentVolume을 경고한다.

현재 metric에는 개별 hold owner/role, cleanup reason class, journal별 age가 label로 노출되지 않는다.
상세 원인은 해당 Volume/Move status, Event와 log에서 확인한다.

경보의 지속 시간은 paging noise를 줄이는 관찰 조건일 뿐 state transition이나 GC timer가 아니다. Alert가
해제되어도 API receipt와 fresh absence proof가 없으면 cleanup과 capacity release는 완료되지 않는다.

영구 node/disk 손실은 자동 정상화 metric으로 감추지 않고 `Blocked`/cleanup `NeedsReview`와 보존된 hold로 계속 노출한다.
HA/replication, RWX, snapshot과 hard-quota 사용량은 0.4 metrics 계약 범위 밖이다.
