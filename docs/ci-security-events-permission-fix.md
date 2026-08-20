# CI Fix: Missing `security-events: write` Permission for SARIF Upload

Repo: `Ali-stack261/stock`
Commit checked: `bdb6b86`

## Two separate things happening in this log — only one is a bug

### 1. Trivy's `exit code 1` — expected, correct behavior, NOT a bug

```
[jar] Detecting vulnerabilities...
[node-pkg] Detecting vulnerabilities...
[python-pkg] Detecting vulnerabilities...
Error: Process completed with exit code 1.
```

This confirms the entire registry migration chain from the last three fixes worked —
Trivy is genuinely pulling and scanning the pushed image (OS detection, package
scanning across three ecosystems). The workflow deliberately sets
`exit-code: "1"` + `severity: "HIGH,CRITICAL"` specifically so the build fails when
real vulnerabilities are found — that's the entire point of having a scan gate. This
is the scanner doing its job, not a CI configuration problem. **Scroll up in the same
Trivy log** — above what's visible in the screenshot — for the actual vulnerability
table (CVE IDs, severities, affected packages). That's real, substantive information
about the image, separate from the CI plumbing issue below.

### 2. `upload-sarif` failing — a real regression, introduced by the previous fix

```
Warning: This run of the CodeQL Action does not have permission to access the
CodeQL Action API endpoints... please ensure the workflow has at least the
'security-events: read' permission.
```

The previous fix added an explicit `permissions:` block to unblock GHCR pushes:
```yaml
    permissions:
      contents: read
      packages: write
```
Declaring *any* explicit `permissions:` block on a job revokes all of GitHub's
implicit default grants for that job — including ones not mentioned. `upload-sarif`
needs `security-events: write` to upload scan results, and that wasn't included, so
it silently lost access the moment the block was added for the packages fix.

## The fix

```diff
     permissions:
       contents: read
       packages: write
+      security-events: write
```

## How to apply

```powershell
cd 'C:\Users\Alim1\OneDrive\Desktop\stock'
```
Open `.github/workflows/ci.yml`, add `security-events: write` to the `build-and-scan`
job's `permissions:` block (three lines total now). Then:
```powershell
git add .github/workflows/ci.yml
git commit -m "fix: add security-events:write permission for SARIF upload"
git push origin master
```

## What to expect after this lands

- `upload-sarif` should succeed
- Trivy will likely still exit with code 1 and fail the job — **that's correct**, not
  something to "fix" by removing `exit-code: "1"`. The real next step is scrolling up
  in the Trivy log to see which specific CVEs it found, and addressing those (base
  image update, dependency bump) rather than suppressing the gate that's working as
  designed.

## Net effect

Fourth and hopefully final fix in the GHCR migration chain (login → lowercase tag →
package-creation permission → this). Once this lands, `build-and-scan`'s only
remaining "failure" should be Trivy legitimately finding vulnerabilities to report —
worth treating as real findings to act on, not CI noise to silence.
