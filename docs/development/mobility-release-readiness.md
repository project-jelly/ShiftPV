# Mobility release readiness

2026-10-15 UTC 검증 기록. 구현 단계와 PR은 [완료 계획](mobility-stabilization-plan.md),
현재 동작 계약은 [volume mobility](../spec/volume-mobility.md)가 소유한다.
이 기록은 소스·테스트·CI의 판정이며 이미지/Chart 발행 또는 운영 배포 증거가 아니다.

## 책임과 복구 경계

```text
Admission: owner pin / placement hold
Events + periodic LIST
  -> Volume queue [2 workers; 같은 Volume 직렬 처리]
     -> pass API 예산 [30s; 더 짧은 부모 deadline 유지]
        +-- Owner placement: exact binding + 정상 owner -> 같은 held Pod 복구
        +-- Move: 준비 hold -> 중단 -> 사용량 -> 실행 승인 -> owner CAS
        |    +-- Pool UID lock: 최신 용량 원장 / 승인 / owner 유입 직렬화
        |    +-- Usage Pod: 측정 -> 결과 CAS -> 정확한 Pod 정리
        |    +-- Jobs: copy -> checksum verify -> promote
        |    +-- Node / 기존 bound Job: 승인된 cleanup + receipt
        +-- 정산: cleanup 실행 정산 -> fresh absence -> hold/activeMove 해제
                  -> helper 책임 종료 -> finalizer -> 보존 기간 뒤 journal GC
```

owner CAS 전에는 source, 이후에는 destination이 권한을 갖는다. API journal이 승인과 완료
증거를 보관한다. Queue가 외부 writer까지 잠그지는 않으므로 fresh read·UID·CAS·NodePublish
권한 검사를 유지한다. 정상 Pool의 기존 copy 작업은 자기 backing을, 신규 할당은 Pool 간
독립성까지 검증한다. 준비 hold는 논리 예약이며 외부 writer를 제한하는 hard quota가 아니다.

timeout은 API가 쓰기를 거절했다는 증거가 아니다. 만료된 pass는 후속 작업과 진단 쓰기를
멈추고 다음 pass가 저장된 사실을 재조회한다. 읽기 전용 rsync source의 삭제 요청·owner GC와
destructive cleanup Job의 완료·부재 증거는 서로 다른 수명 조건이다.

## 검증 판정

| 관점 | 통과해야 하는 경계 | 증거 |
|---|---|---|
| 안전성 | 준비 예약 전 source 유지, quiesced 사용량·공간 재검증, owner CAS·publish 전 source cleanup 금지 | PR #201–204, preparation/native-cleanup Kind suites |
| 진행성 | 같은 Pod/Move 복구, 비동기 usage/cleanup, API 지연에서 worker 반환 | PR #199, #205–210; 실제 HTTP 지연 경계 TC |
| 동시성 | 용량 원장·owner 유입 직렬화, 같은 Volume max 1, accepted write 응답 유실 후 재조회 | ledger/dispatcher/expiry 경계 TC |
| 기록 수명 | executor 정산·fresh absence 전 hold 유지, 결과 저장 전 helper 유지, terminal/finalizer/GC 정산 | cleanup Job 재진입·source usage·terminal cleanup TC |

구체적인 파일과 gate는 [테스트 지도](testing.md#four-review-criteria)를 따른다.
[PR #211](https://github.com/project-jelly/ShiftPV/pull/211)의
[CI 38756344980](https://github.com/project-jelly/ShiftPV/actions/runs/38756344980)에서
head `302c65c`의 controller 이미지
`sha256:a738616d4b8f0ddbcba2b782a47a26df4d9e575f37eec9cebe95d4ee402c95eb`가
6개 filesystem qualification을 통과했다. 원본 rsync/라이브러리 hash 유지와 정확한 시험 container
부재도 확인했다. `rsync-qualification` artifact의 `result.json`과 로그가 실행 증거다.
이 문서와 함께 추가한 038/039 TC는 해당 PR의 필수 `verify`·Kind mobility `g1` CI를 통과해야 머지한다.

## 038 / 039 운영 검증

- **정상 순서:** cordon → Move cleanup 완료·Volume Ready/activeMove 해제·destination Pod Ready → drain.
  drain 전후 destination Pod, PVC/PV/Volume, Move identity와 고유 payload를 비교한다.
- **잘못된 순서의 복구:** drain으로 source consumer가 먼저 사라졌을 때, 정상 source가 여전히
  exact owner이고 미정산 Move가 없다면 uncordon 후 같은 held replacement UID가 복구되는지 확인한다.
- **VMI owner:** 실제 admission JSON patch와 재시작한 reconciler로 owner reference·사용자 제약을
  보존하는 TC다. 실제 KubeVirt VM 실행 또는 live migration 시험은 아니다.

Kind에서는 격리된 시험 workload selector로 drain한다. drain-first 자동 이동이나 운영 노드 전체의
모든 workload eviction을 보장하는 시험으로 확대 해석하지 않는다.

## 성능 관측

동일 arm64 Alpine/rsync 3.5.0 이미지
`sha256:990461c33e4b20f3ae8280727761f6ba28431c50dcc3a0fdbac40cc6260c88d1`에서
baseline과 copy-only zstd1을 각 3회 교차 측정했다. source cache는 warm, destination은 매회 비웠고
CPU quota는 없었다. sparse는 1GiB 논리 파일에 16MiB random island 4개, dense는 64MiB random이다.
32MiB/s는 rsync socket pacing이며 실제 NIC shaping이나 WAN 재현이 아니다.

| 조건 | copy 중앙값, 초 | verify 중앙값, 초 |
|---|---:|---:|
| 제한 없는 sparse | 0.94 → 0.97 | 0.44 → 0.41 |
| 제한 없는 dense | 0.24 → 0.23 | 0.14 → 0.14 |
| 32MiB/s sparse | 33.59 → 2.41 | 0.51 → 0.43 |
| 32MiB/s dense | 2.18 → 2.19 | 0.14 → 0.15 |

24회 모두 SHA256·checksum dry-run·논리 크기 검사를, sparse 12회는 할당 검사를 통과했다. 제한 전송의 sparse
copy+verify 중앙값은 34.12→2.86초, 수신 protocol bytes는 1,074,004,085→67,171,280이었다.
dense 총시간은 2.32→2.33초이며 sender CPU 비용이 늘었다. 이 결과는 Home 성능이나 총 Move
중단 시간 예측이 아니다. checksum 읽기와 fsync는 유지한다.

- **Queue:** PR #210의 실제 두 worker·queue·Registry와 HTTP 지연 fixture에 200ms 예산을 주입했다.
  3회 중앙값은 정상 Volume Add→Get 202.220ms, Get→plan 저장 1.818ms다. 이전 dispatcher는
  1초 관측창에서 dequeue·저장이 없었다. 저장은 UID/RV fake API이며 운영 SLO나 개선율이 아니다.
- **Startup:** 이전 Kind 시험의 4개 copy/promote Pod는 초 단위 timestamp상 각각 약 1초였다.
  표본·해상도가 부족하며 이번 후보의 startup 개선 증거로 사용하지 않는다.
- **Qualification:** 실제 CI 이미지의 baseline/zstd1 × clean/TERM/KILL은 metadata·SHA·sparse·
  실제 partial write·전체 process group 종료·동일 destination 재시도·container 부재를 검사한다.
  이는 성능 측정, 부모 프로세스만 종료한 경우, 물리 전원 차단 시험과 구분한다.

## 별도 운영 검증

[업그레이드 순서](versioning.md#planned-mobility-upgrades)에 따라 CRD/RBAC, Node capability,
호환 chart/controller/helper 조합과 작업 정산을 확인해야 한다. 현재 저장소의 버전 핀만으로
이번 구현이 기존 공개 이미지에 포함됐다고 판단하지 않는다.

실제 LVM/filesystem 조합, Home의 부하·대용량 이동, 실제 KubeVirt VM 복구, 물리 전원 손실은
이번 소스/격리 CI 증거 밖이다. 공통 bulk LIST 지연, 같은 Volume 내 history 공정성, Node 커널
I/O도 30초 pass 예산의 보장 범위가 아니다. 운영 승인 gate는 [testing](testing.md)을 따른다.
