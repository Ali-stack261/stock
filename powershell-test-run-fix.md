# Running the Test Suite on Windows / PowerShell

## Verified independently
The latest commit (`06e3be9` — Kafka producer fix) was already run and confirmed
passing in a separate environment:
```
Ran 12 tests in 5.006s
OK
```
So the code fix itself is good — this doc is only about getting your local
PowerShell shell to run it.

## Try this first
```powershell
cd 'C:\Users\Alim1\OneDrive\Desktop\stock'
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

## Common PowerShell gotchas

| Issue | Fix |
|---|---|
| Don't quote the python path unless it has spaces | Use `python` or `py`, unquoted |
| `python` not recognized | Try `py -3 -m unittest discover -s tests -v` (the `py` launcher is more reliable on Windows) |
| `&&` chaining fails on older PowerShell | Use `;` instead, or put commands on separate lines |
| Using a venv and the exe won't invoke directly | Activate it: `.\venv\Scripts\Activate.ps1` |
| Script execution blocked | `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` (session-only, safe) |

## If it still fails
Share the exact error text and it can be narrowed down further from there.
