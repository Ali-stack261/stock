# CI Fix: Table-Format Trivy Step Skipped When SARIF Step Fails

Repo: `Ali-stack261/stock`
Commit checked: `1d2a2c3` — table-format step confirmed present in the workflow, but
missing the condition needed to actually run in the scenario that matters most.

## The bug, confirmed from the actual log

The pasted log for the newest run shows only the original SARIF-format Trivy step
(`"Building SARIF report with all severities"`), ending in `exit code 1` as expected
— no second table-format step output appears anywhere in the log at all.

Checked the workflow directly:
```yaml
      - uses: aquasecurity/trivy-action@master
        with:
          format: "sarif"
          output: "trivy-results.sarif"
          severity: "HIGH,CRITICAL"
          exit-code: "1"

      - name: Trivy scan (human-readable table for logs)
        uses: aquasecurity/trivy-action@master
        with:
          format: "table"
          severity: "HIGH,CRITICAL"
          exit-code: "0"
```
The new table step has no `if:` condition. GitHub Actions' default behavior for any
step without one is to skip it if an earlier step in the same job failed. Since the
SARIF step exits with code 1 whenever it finds HIGH/CRITICAL vulnerabilities (which is
its whole purpose), the table step gets silently skipped in exactly the one scenario
where seeing the table actually matters — when there are findings to look at.

This is the same category of issue already fixed once in this workflow for
`upload-sarif`, which does have `if: always()` for precisely this reason — just
missed on this newer step.

## The fix

```diff
       - name: Trivy scan (human-readable table for logs)
         uses: aquasecurity/trivy-action@master
+        if: always()
         with:
           image-ref: ghcr.io/${{ steps.lowercase.outputs.owner }}/${{ matrix.image }}:${{ github.sha }}
           format: "table"
           severity: "HIGH,CRITICAL"
           exit-code: "0"
```

## How to apply

```powershell
cd 'C:\Users\Alim1\OneDrive\Desktop\stock'
```
Open `.github/workflows/ci.yml`, add `if: always()` to the `Trivy scan (human-readable
table for logs)` step (right after the `uses:` line, matching the same pattern already
used on `upload-sarif` below it). Then:
```powershell
git add .github/workflows/ci.yml
git commit -m "fix: run table-format Trivy step even when SARIF step fails"
git push origin master
```

## In the meantime, the findings are already accessible

`upload-sarif` already has `if: always()` correctly, and permissions are all in
place — the vulnerability findings from this exact run are already browsable in the
repo's **Security → Code scanning alerts** tab right now, without waiting for this fix.

## Net effect

Small, one-line fix. Once applied, both Trivy steps will run on every scan regardless
of outcome — the SARIF step still gates the build (`exit-code: "1"`), the table step
is purely for log visibility (`exit-code: "0"`) and will now actually show up when
there's something to show.
