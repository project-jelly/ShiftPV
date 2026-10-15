# Model and runtime checks

모델은 상태 전이의 내부 일관성을, primitive 검사는 대상 runtime의 전제를 확인한다.
실제 Controller 전체 동작과 운영 환경의 내구성은 별도 검증한다.

## Run

```bash
make v04-model
make v04-kubernetes-primitives
make v04-filesystem-primitives IMAGE=shiftpv:dev
```

| 검사 | 확인하는 것 | 확인하지 않는 것 |
|---|---|---|
| `v04-model` | Move와 Volume 삭제의 안전 invariant, 재시작 후 수렴, commit 경계, 보수적 용량 hold | Kubernetes API와 실제 filesystem effect |
| `v04-kubernetes-primitives` | CR generation/status 분리, deletionTimestamp 뒤 status journal, finalizer와 owner-referenced Job 수명주기 | ShiftPV CRD와 Controller 구현 |
| `v04-filesystem-primitives` | 실제 image의 baseline/zstd copy에서 metadata·checksum·sparse 보존, clean/TERM/KILL 후 process 정산과 재시도 | 전원 차단 뒤 durability, 실제 node filesystem 조합, 성능 |

Filesystem 검사는 로컬에 빌드한 `IMAGE`를 immutable image ID로 고정한다. Controller image CI에서도
실행하며, 결과와 실패 진단은 `.tmp/rsync-qualification/`에 남긴다. 원래 rsync와 공유 라이브러리가
검사 도구 설치로 바뀌면 실패한다. 총 실행·정리 예산은 300초이며, 시험 container만 제거한다.

모델은 stale, invalid, incomplete observation을 재시도 가능한 증거 부족으로 취급한다. Exact identity가
충돌하거나 삭제 intent 없이 경로가 사라지면 `NeedsReview`에 해당하는 격리 상태로 남기고 destructive
transition을 허용하지 않는다.

모든 production 보증은 같은 계약을 실제 CRD, Controller, node-local lock, helper effect에 구현한 뒤
unit, Linux integration, isolated Kind fault injection, real-node unclean OS reboot와 soak gate를 통과해야 성립한다.
