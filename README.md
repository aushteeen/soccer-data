# soccer-data

Daily mirror of ESPN soccer match stats (shots on target, fouls, yellow/red cards, referee) for the
competitions listed in `FETCH_ARGS` (or the `core` group when that file is empty). Built by
`.github/workflows/mirror.yml` using `scripts/fetch_espn.py` from the soccer-match-stats-predictor skill.

Files: `espn/matches.csv` (played matches), `espn/fixtures.csv` (next 8 days), `espn/coverage.csv`,
`espn/state.json`, `espn/MIRRORED_AT`. Single-commit history; data is public (ESPN).
