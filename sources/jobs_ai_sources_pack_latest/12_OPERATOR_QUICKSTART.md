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

## 6. Reopen or open a prior session
```bash
jobs-ai session reopen 1
jobs-ai open data/exports/<session-manifest>.json 2
```

## 7. Review-first browser assist
```bash
jobs-ai application-assist data/exports/<session-manifest>.json --prefill --launch-order 1
```

Optional post-browser logging:
```bash
jobs-ai application-assist data/exports/<session-manifest>.json --prefill --launch-order 1 --log-outcome
jobs-ai application-log --manifest data/exports/<session-manifest>.json --launch-order 1 --status applied --notes "manual submit after review"
```

## 8. Mark outcomes in the DB
```bash
jobs-ai session mark applied --manifest data/exports/<session-manifest>.json --all
jobs-ai track list --status applied
jobs-ai stats --days 7
```

## 9. Inspect backend state
```bash
jobs-ai db backend-status
jobs-ai db ping
jobs-ai db status
```
