# Operator Quickstart

## 1. Install and initialize
```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
cp .env.example .env
jobs-ai init
jobs-ai db init
```

## 2. Daily discover-first workflow
```bash
jobs-ai run "python backend engineer remote" --limit 25
jobs-ai session recent
jobs-ai session inspect 1
```

## 3. Daily registry-first workflow
```bash
jobs-ai run "python backend engineer remote" --use-registry --limit 25
jobs-ai run "python backend engineer remote" --use-registry --us-only --limit 25
jobs-ai session recent
jobs-ai session inspect 1
```

## 4. Modular workflow when you want the stages separate
```bash
jobs-ai discover "python backend engineer remote" --collect --import
jobs-ai session start --limit 25
jobs-ai session recent
```

## 5. Remote-safe open flow
```bash
jobs-ai session start --limit 20 --open --executor remote_print
```

## 6. Inspect or open one job
```bash
jobs-ai check-url https://boards.greenhouse.io/example/jobs/1234567890
jobs-ai check-url https://boards.greenhouse.io/example/jobs/1234567890 --inspect
jobs-ai inspect 123
jobs-ai open 123
jobs-ai open --manifest data/exports/<session-manifest>.json --index 1
```

## 7. Review-first browser assist for one launchable item
`--launch-order` is counted across launchable items only. It is not always the same as manifest index.

```bash
jobs-ai application-assist data/exports/<session-manifest>.json
jobs-ai application-assist data/exports/<session-manifest>.json --prefill --launch-order 1
jobs-ai application-assist data/exports/<session-manifest>.json --prefill --launch-order 1 --log-outcome
jobs-ai application-log --manifest data/exports/<session-manifest>.json --launch-order 1 --status applied --notes "manual submit after review"
```

## 8. Direct URL and status flows
```bash
jobs-ai apply-url https://boards.greenhouse.io/example/jobs/1234567890
jobs-ai applied 123
jobs-ai invalid-location https://example.com/jobs/non-us-role
jobs-ai track mark interview 123
```

## 9. Batch mark and stats
```bash
jobs-ai session mark applied --manifest data/exports/<session-manifest>.json --indexes 1
jobs-ai track list --status applied
jobs-ai stats --days 7
```

## 10. Maintenance and backend checks
```bash
jobs-ai maintenance supersede-duplicates --dry-run
jobs-ai maintenance mark-invalid-location --us-only --dry-run
jobs-ai db backend-status
jobs-ai db ping
jobs-ai db status
```
