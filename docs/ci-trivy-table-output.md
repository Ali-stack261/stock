# CI Enhancement: Human-Readable Trivy Table in the Actions Log

Repo: `Ali-stack261/stock`
Commit checked: `0a20b69` — `security-events: write` fix confirmed correctly landed
(verified: `contents: read`, `packages: write`, and `security-events: write` are all
present in the `build-and-scan` job's `permissions:` block).

## Why this is needed

The existing Trivy step uses `format: "sarif"`, which writes structured JSON to
`trivy-results.sarif` for `upload-sarif` to consume — it does **not** print a
human-readable vulnerability table to the console. Confirmed directly from the full
log text: it goes straight from `[python-pkg] Detecting vulnerabilities...` to the
severity warning to `exit code 1`, with no table in between. There's nothing to
scroll up and find — SARIF format simply doesn't produce console table output.

**Where the findings actually show up today:** GitHub's **Security → Code scanning
alerts** tab, now that `upload-sarif` has permission to actually upload them.

**This doc adds a second option**: a table-format Trivy step so the same findings are
also visible directly in the Actions log, without needing to click through to the
Security tab.

## The fix

Add a second `trivy-action` step immediately after the existing one. Keep the
original SARIF step as-is (it's the actual build gate and Security-tab source) — this
new step is purely for log visibility, so it must not fail the build a second time.

```diff
       - uses: aquasecurity/trivy-action@master
         with:
           image-ref: ghcr.io/${{ steps.lowercase.outputs.owner }}/${{ matrix.image }}:${{ github.sha }}
           format: "sarif"
           output: "trivy-results.sarif"
           severity: "HIGH,CRITICAL"
           exit-code: "1"

+      - name: Trivy scan (human-readable table for logs)
+        uses: aquasecurity/trivy-action@master
+        with:
+          image-ref: ghcr.io/${{ steps.lowercase.outputs.owner }}/${{ matrix.image }}:${{ github.sha }}
+          format: "table"
+          severity: "HIGH,CRITICAL"
+          exit-code: "0"
+
       - uses: github/codeql-action/upload-sarif@v3
         if: always()
```

**Why `exit-code: "0"` here specifically:** the SARIF step above already enforces the
real gate (`exit-code: "1"`) — if it found HIGH/CRITICAL vulnerabilities, the job
already failed before reaching this step. This second step is read-only visibility;
it shouldn't independently fail the build a second time for the same findings.

**Why this step doesn't need the image re-pulled or rebuilt:** Trivy scans the
already-pushed `ghcr.io` image by reference, same as the SARIF step — no rebuild,
just a second scan pass (which will hit Trivy's local vulnerability DB cache from the
first run, so it should be fast).

## How to apply

```powershell
cd 'C:\Users\Alim1\OneDrive\Desktop\stock'
```
Open `.github/workflows/ci.yml`, add the new step shown above between the existing
`aquasecurity/trivy-action@master` step and `github/codeql-action/upload-sarif@v3`.
Then:
```powershell
git add .github/workflows/ci.yml
git commit -m "ci: add human-readable Trivy table output alongside SARIF scan"
git push origin master
```

## What to expect after this lands

- The job's overall pass/fail status is unchanged — still gated by the original SARIF
  step's `exit-code: "1"`
- A new step in the Actions log will show the actual CVE table: package name,
  installed version, vulnerability ID, severity, fixed version (if available)
- Findings are also browsable in **Security → Code scanning alerts**, now that
  `security-events: write` is in place

## Net effect

Doesn't change what gets gated or blocked — purely adds visibility so the actual
vulnerability findings are readable in two places (Actions log and Security tab)
instead of only being consumable via the Security tab.
