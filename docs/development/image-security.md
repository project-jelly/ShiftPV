# CI and image security policy

## Pull requests

`Dependency Review` rejects newly introduced dependencies with HIGH/CRITICAL
advisories. `Vulnerability Check` runs pinned `govulncheck` against all Go packages.
The existing race, Helm, mount, Kind mobility and public-artifact checks remain.
Dependabot opens weekly Monday 09:00 Asia/Seoul updates for Go, Actions and the
production Dockerfile. Only minor/patch **version updates** are grouped; security
updates retain their independent behavior. The E2E fault-helper Dockerfile extends
a locally built `shiftpv:dev`, so it is not a registry dependency update target.

Repository settings (Dependabot security updates, secret scanning/push protection,
CodeQL configuration and required checks) are administered separately. New check
names must be registered in branch rules only after their first successful runs.
A successful CodeQL analysis does not itself enforce an alert severity policy.

## Production release gate

Image and chart releases start only after `CI` succeeds for a main push from this
repository. The release checks out that run's exact SHA. A run-scoped artifact
binds the original push range and repository to that SHA; PR, failed, cancelled,
skipped and manually dispatched CI runs cannot start a production release.

Each component remains an independent release unit. The resolver compares the
current version with published component tags, so the next successful main CI can
publish a version bump whose earlier CI was cancelled. Already published,
unchanged versions are no-ops; rollbacks and conflicting changed tags fail.
Expired/missing release-context artifacts fail closed; trigger a fresh main CI by
an ordinary reviewed push rather than bypassing identity validation.

Images are built once for native amd64 and arm64 runners and pushed by digest.
Both immutable candidates must pass the shared scan and signed-evidence checks
before the publish job creates version/latest tags from those same digests.
BuildKit attaches SBOM and maximum SLSA provenance to each platform image index.
Separately, signed custom in-toto release evidence records the built source SHA,
trusted CI run, signing workflow revision and invocation. It does not label a
`workflow_run` invocation as the GitHub Actions SLSA workflow build type, whose
[documented supported events](https://github.com/actions/buildtypes/blob/main/workflow/v1/README.md)
do not include that trigger.

Both per-platform and final consumer-index evidence are verified against the
repository, signing workflow and exact predicate before promotion. The in-toto
statement's subject binds the image name and digest; `gh attestation verify` checks
that binding cryptographically for the exact OCI digest. The final index is pushed
by digest first; promotion verifies version/latest retain that digest. An existing
version image blocks rebuilding or overwriting, including partial-release retries;
investigate and complete that release explicitly instead.

This is repository-authored evidence, not a SLSA-level certification. Consumer-side
verification and GitOps admission are separate controls; this change does not
enforce them in the cluster.

### Signed release-evidence schema

The versioned predicate type identifier is
`https://project-jelly.github.io/attestations/release-evidence/v1`. It identifies the
schema below; it does not assert that a documentation page is hosted at that URL.
All fields are required strings within the following objects:

```json
{
  "source": {"repository": "https://github.com/owner/repo", "commit": "<built 40-character SHA>"},
  "ci": {"runId": "<successful trusted CI run>", "runAttempt": "<CI attempt>"},
  "workflow": {"ref": "owner/repo/.github/workflows/release.yaml@refs/heads/main", "commit": "<workflow 40-character SHA>"},
  "invocation": {"id": "https://github.com/owner/repo/actions/runs/<run>/attempts/<attempt>", "event": "workflow_run", "runnerEnvironment": "github-hosted"}
}
```

The shared helper accepts `workflow_run` or a separately guarded
`workflow_dispatch`, requires GitHub-hosted runners, and keeps built source and
workflow revisions distinct. Trusted successful-main-CI validation remains a
separate mandatory gate. Verification requires an exact match for the entire
predicate, not only a matching source SHA. The CLI accepts both known verified
statement field spellings but rejects conflicting type fields.

Consumers must explicitly select this predicate type when verifying evidence:

```sh
gh attestation verify "oci://ghcr.io/project-jelly/shiftpv-controller@sha256:<digest>" \
  --repo project-jelly/ShiftPV \
  --signer-workflow project-jelly/ShiftPV/.github/workflows/release-images.yaml \
  --predicate-type https://project-jelly.github.io/attestations/release-evidence/v1 \
  --deny-self-hosted-runners --format json
```

Chart publication still waits for both published image platforms, preserves
immutable chart packages/checksums, and verifies the public chart repository.
The existing manually dispatched `codex/v04-*` development candidate job remains
limited to `candidate-<sha>` tags; it cannot publish production version/latest tags.

## Image scan policy

Release candidates and daily released-image rescans use the same local action:

- Trivy CLI `v0.74.0`, with full commit pins for external Actions.
- OS and library vulnerabilities plus secrets; HIGH and CRITICAL block the job,
  including vulnerabilities without a fix. No ignore file is applied.
- Missing/invalid scan results and scanner/database failures fail closed.
- JSON, SARIF, text and immutable digest/platform evidence are retained for
  30 days even when findings fail the job. If scanning fails before producing a
  report, the target and any partial output remain available instead.
- SARIF upload failures are workflow failures; artifacts remain available.
- Exceptions require a reviewed finding ID, rationale, owner and expiry. There
  are no active exceptions in this configuration.

The daily workflow runs at 00:43 UTC (09:43 Asia/Seoul) and supports manual runs;
GitHub may delay schedules. It verifies the latest stable public Helm chart and
scans both component digests for both platforms. These are chart-selected images,
not necessarily production's deployed digests. CSI sidecars and historical
supported releases remain outside this scan. Daily findings do not modify images
or running workloads. Handle scanner failures separately from actual findings.

Controller and Node use Alpine, with `rsync` in Controller and util-linux mount
utilities in Node. Preserve those runtime contracts when updating the base image.
Base updates require a reviewed component release and existing mount/mobility
checks. A scan is point-in-time evidence, not a guarantee against future findings.

## Basis and local choices

[GitHub secure use](https://docs.github.com/en/actions/reference/security/secure-use)
and [OWASP CI/CD guidance](https://cheatsheetseries.owasp.org/cheatsheets/CI_CD_Security_Cheat_Sheet.html)
support immutable Actions, least privilege and protected release paths.
[NIST SSDF PO.4](https://csrc.nist.gov/pubs/sp/800/218/final) motivates explicit
verification criteria; [NIST SP 800-204D](https://csrc.nist.gov/pubs/sp/800/204/d/final)
covers supply-chain checks in CI/CD. [SLSA verification guidance](https://slsa.dev/spec/v1.2/verifying-artifacts)
motivates checking provenance rather than merely generating it.
The selected tools, HIGH/CRITICAL threshold, weekly updates, daily scans and
30-day retention are project policy choices, not mandatory numbers in those documents.
