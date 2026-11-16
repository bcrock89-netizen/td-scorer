# Anytime TD Board

A weekly anytime-touchdown scorer board (QB rushing, RB, WR, TE), rebuilt
automatically by GitHub Actions and published with GitHub Pages. It runs
whether or not your own computer is on.

## When it updates

| When (Eastern) | What |
|---|---|
| Tuesday ~3 PM | Fresh nflverse data (Monday night included), next week's projections, anytime-TD lines |
| Friday ~10 PM | Re-run with final injury designations, updated lines; Thursday's game drops off |
| Any time | Actions tab, then "Update TD board", then "Run workflow" (untick the odds box to save credits) |

GitHub cron runs on UTC, so during daylight time (Sep to early Nov) each run
lands an hour later in Eastern. GitHub can also start scheduled runs up to
about an hour late.

## Odds API credits

Each pull is 1 market (anytime TD) x 1 region, so about **1 credit per game,
roughly 13-16 per pull**. Two scheduled pulls a week is about 120 a month,
out of the free tier's 500, which this shares with the local WR pipeline's
pulls. Each run's log prints the credits remaining (open the run, then the
"Pull anytime-TD lines" step).

## Files

| Path | What |
|---|---|
| `R/fetch_data.R` | Downloads play-by-play, schedules, rosters, injuries from nflverse |
| `R/fetch_odds.R` | Pulls anytime-TD lines into `data/odds/` (merges with earlier pulls that week) |
| `R/td_scorer_model.R` | The model. A copy of `scripts/td_scorer_model.R` in the NFL MODEL folder |
| `R/build_site.R` | Turns `docs/weeks/*.csv` into `docs/index.html` |
| `site/template.html` | The page's design and JavaScript (edit this, not `docs/index.html`) |
| `.github/workflows/update-site.yml` | Schedule + steps |
| `docs/weeks/` | Every week's projections (kept, so the week dropdown has history) |
| `data/odds/` | Every week's lines (kept, so the Friday pull can merge with Tuesday's) |

## Changing the model

The model file is shared with the local NFL MODEL folder. After editing
`scripts/td_scorer_model.R` there, copy it here and push:

```bash
cp "../scripts/td_scorer_model.R" R/td_scorer_model.R
git add R/td_scorer_model.R && git commit -m "Update model" && git push
```

## Off-season

After the final regular-season week there are no unplayed games, so the model
stops with "No unplayed games" and the run shows as failed. Disable the
workflow (Actions tab, then "Update TD board", then the "..." menu, then
"Disable workflow") until next season, and turn it back on in September.
