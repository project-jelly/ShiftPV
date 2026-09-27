# Development

0.4 변경은 contract, implementation, fault evidence를 같은 변경 단위로 닫는다.

| 문서 | 책임 |
|---|---|
| [source-layout.md](source-layout.md) | source tree와 package 책임 경계 |
| [testing.md](testing.md) | 설계 preflight부터 운영 승인까지의 evidence gate |
| [image-security.md](image-security.md) | PR 검사, release gate, 이미지 보안 정책과 근거 |
| [versioning.md](versioning.md) | 독립 component와 Chart version, release 순서 |

제품 동작은 [`spec/`](../spec/README.md), 설계 이유는 [`adr/`](../adr/README.md)가 소유한다.
검증 결과에는 실행한 commit, 명령, 환경, resource 최종 상태와 cleanup 결과를 함께 남긴다.
