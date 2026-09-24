# Released image security reports

`Released Image Security Report` is independent of build, test, image release,
chart release and deployment. It runs daily at 00:43 UTC (09:43 Asia/Seoul),
or manually from the Actions tab. GitHub may delay scheduled runs.

## Scope

The existing public-artifact resolver selects the latest stable published Helm
chart, verifies its package against the release checksum, and pins its Controller
and Node images by multi-platform manifest digest. Trivy scans both `linux/amd64`
and `linux/arm64` for OS and application-library vulnerabilities. It does not
rebuild images or enumerate the registry. An incomplete release causes a visible
workflow failure instead of silently scanning an older version.

These are the images selected by the latest public chart, not necessarily the
images deployed in production. Production deployment and digest selection belong
to the separate GitOps repository. Third-party CSI sidecar images are outside
this initial two-component report.

## Policy and results

- Report HIGH and CRITICAL vulnerabilities, including those with no available fix.
- Vulnerability findings do not fail the workflow or block merges/releases/deploys.
- Scanner, database, image-resolution or report-generation failures fail this
  standalone workflow so a missing scan cannot be mistaken for a clean image.
- No ignore file is applied. Any future exception policy needs explicit review,
  a finding ID, a reason and an expiration date.
- JSON, SARIF and the target digest/platform are retained as Actions artifacts
  for 14 days. The release lock is retained separately for reproduction.
- SARIF is uploaded to **Security → Code scanning** with a stable category per
  component/platform. An upload failure leaves a warning and the artifacts;
  it does not discard the report or affect the release pipelines.

Trivy Action uses its built-in database cache and still checks database freshness.
Only the scan job requests `security-events: write`; there are no registry writes,
production credentials, new secrets, PATs or `pull_request_target` triggers.
The workflow's external actions are pinned to full release commit SHAs. Trivy CLI
is pinned to `v0.74.0`; review and update that version and action pins periodically.

## Runtime image maintenance

Controller and Node use Alpine 3.24.2, pinned by the multi-platform base digest
in `build/package/Dockerfile`. Controller includes `rsync`; Node explicitly
installs the util-linux `mount` and `umount` packages. Keep those implementations
when updating Alpine, because the CSI bind-mount behavior must remain compatible.
The Go builder remains independent of the runtime distribution.

Base-image updates require rebuilding the final images and checking both
architectures with Trivy and the existing mount/mobility/upgrade tests. A clean
scan is a point-in-time report, not a guarantee against future vulnerabilities.
Refresh the base pin and package contents through a reviewed component patch
release; the daily report does not rebuild or update deployed images.

## Operations

After the workflow reaches the default branch, run it once from
**Actions → Released Image Security Report → Run workflow** and verify all four
component/platform reports. Do not add this report-only workflow to required
branch checks. Investigate scanner failures separately from reported CVEs.

Existing Dependency Graph, Dependabot Alerts and Dependabot Security Updates
remain separate from this workflow. It does not add Dependabot version-update
configuration or change the existing test/build/release workflows.

Future changes, if needed, are production-digest tracking through GitOps and a
separately approved release gate. Filesystem, secret, IaC and license scanning,
central workflows and a Trivy server are not part of this implementation.

## Official references

- [Trivy Action: image scans and built-in cache](https://github.com/aquasecurity/trivy-action/tree/v0.36.0)
- [Trivy JSON report conversion](https://trivy.dev/docs/latest/references/configuration/cli/trivy_convert/)
- [Uploading SARIF to GitHub](https://docs.github.com/en/code-security/how-tos/find-and-fix-code-vulnerabilities/integrate-with-existing-tools/upload-sarif-file)
