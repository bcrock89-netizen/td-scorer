# Anytime TD scorer model — QB (rushing only), RB, WR, TE.
#
# STANDALONE and SELF-CONTAINED: it sources nothing from R/, needs no
# config.yml, and the WR yards/receptions pipeline doesn't read anything it
# writes. The same file runs locally and in the website repo
# (td-scorer-site/R/td_scorer_model.R is a copy; after editing one, copy it
# over the other).
#
# It only READS cached data:
#   <raw>/pbp/season=*.parquet         nflreadr/nflfastR play-by-play
#   <raw>/schedules/schedules.parquet  slate + Vegas spread/total
#   <raw>/rosters/rosters.parquet      current team + ACT/INA status
#   <raw>/injuries/season=*.parquet    this week's Out designations
#   <odds>/season=X_week=Y.parquet     optional anytime-TD lines
# Locally <raw> is data/raw (written by scripts/01_run_ingestion.R) and
# <odds> is data/raw/odds (written by scripts/09_run_odds.R, whose pull
# already includes anytime TD for every position). Override with the
# TD_RAW_DIR / TD_ODDS_DIR / TD_OUT_DIR environment variables.
#
# Usage:
#   Rscript scripts/td_scorer_model.R                      # next unplayed week
#   Rscript scripts/td_scorer_model.R --season 2026 --week 11
#   Rscript scripts/td_scorer_model.R --backtest           # calibration check on past weeks
#
# Output: <out>/season=X_week=Y.csv (default reports/td_scorer/), plus a
# top-30 printout.
#
# ---------------------------------------------------------------------------
# What it computes, per player, over the LAST 5 WEEKS (a bye makes that 4
# games) and SEASON-TO-DATE, using only plays before the target week:
#
# 1. High-leverage opportunities per game: inside-5 carries, inside-10
#    carries, inside-10 targets, end-zone targets (yardline_100 - air_yards
#    <= 0). These are rolled into one "high-leverage xTD per game" number by
#    weighting each look by the league TD rate for that kind of look
#    (estimated from all PRIOR seasons in the cache, so no leakage). An
#    inside-5 carry scores roughly twice as often as a carry from the 8,
#    so a plain count would treat very different looks as equal.
# 2. Red-zone share: player's carries+targets inside the 20 / his team's
#    carries+targets inside the 20, in the games he played.
# 3. Team scoring environment: Vegas implied team total from the schedule's
#    spread/total. Team red-zone trips per game is reported alongside and
#    used as the fallback when no line exists.
# 4. Opponent red-zone TD rate allowed: TD drives / red-zone trips allowed,
#    season-to-date, shrunk toward the league rate (15 pseudo-trips).
#
# Composite score (0-100) = 35% high-leverage xTD/game percentile
#                         + 25% red-zone share percentile
#                         + 25% implied team total percentile
#                         + 15% opponent RZ TD rate allowed percentile
# Percentiles are within this week's slate, so it ranks players within a
# week; don't compare scores across weeks.
#
# Projected TD probability (meant to be a real probability):
#   lambda = (blended all-field xTD/game)
#            x sqrt(implied total / team's own season points per game)
#            x sqrt(opponent shrunk RZ TD rate / league rate)
#   then a logistic recalibration of log(lambda), fit only on the PREVIOUS
#   season's replay. Out-of-sample on 2024-2026 (7,800 player-games), each
#   predicted-probability decile landed within ~3 points of the actual rate
#   except the lowest (6% predicted vs 9% actual); Brier skill 6.8% vs a
#   flat base rate. The composite's top quintile scored 40%, bottom 11%.
#
# Odds: anytime-TD lines are almost always "Yes"-only, so the book's
# probability still includes its margin (it can't be devigged without a
# "No" price). "edge" = model prob - that book prob, so it's conservative:
# a book's true fair price is a bit LOWER than the number shown.
# ---------------------------------------------------------------------------

suppressMessages({
  library(dplyr)
  library(tidyr)
  library(arrow)
})

log_msg <- function(...) message(format(Sys.time(), "[%Y-%m-%d %H:%M:%S] "), ...)
options(width = 200)  # keep the printed table on one line per player

RAW_DIR  <- Sys.getenv("TD_RAW_DIR", "data/raw")
ODDS_DIR <- Sys.getenv("TD_ODDS_DIR", file.path(RAW_DIR, "odds"))
OUT_DIR  <- Sys.getenv("TD_OUT_DIR", file.path("reports", "td_scorer"))

RECENT_WEEKS   <- 5     # "last 4-5 weeks": 5 calendar weeks, so 4 games with a bye
RECENT_WEIGHT  <- 0.6   # blend: 60% recent window, 40% season-to-date
MIN_GAMES      <- 2     # season-to-date games needed to be scored at all
OPP_SHRINK_K   <- 15    # pseudo-trips of league-average RZ TD rate added to each defense
W_HL <- 0.35; W_SHARE <- 0.25; W_ENV <- 0.25; W_OPP <- 0.15
POSITIONS <- c("QB", "RB", "WR", "TE")

# --- CLI ---------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
arg_value <- function(flag) {
  i <- match(flag, args)
  if (is.na(i) || i == length(args)) return(NA_integer_)
  as.integer(args[i + 1])
}
opt_season <- arg_value("--season")
opt_week   <- arg_value("--week")
do_backtest <- "--backtest" %in% args

# --- Data --------------------------------------------------------------------
PBP_COLS <- c("season", "week", "game_id", "season_type", "posteam", "defteam",
              "play_type", "rush_attempt", "pass_attempt", "sack", "qb_kneel",
              "two_point_attempt", "rusher_player_id", "receiver_player_id",
              "yardline_100", "air_yards", "touchdown", "td_team", "td_player_id",
              "fixed_drive", "fixed_drive_result")

pbp_files <- list.files(file.path(RAW_DIR, "pbp"), pattern = "\\.parquet$", full.names = TRUE)
if (length(pbp_files) == 0) stop("No pbp cache in ", RAW_DIR, "/pbp — run ingestion first.")

log_msg("Reading pbp cache (", length(pbp_files), " seasons)")
pbp <- open_dataset(pbp_files) |>
  select(all_of(PBP_COLS)) |>
  filter(season_type == "REG") |>
  collect()

schedules <- read_parquet(file.path(RAW_DIR, "schedules", "schedules.parquet")) |>
  filter(game_type == "REG")

# One row per offensive opportunity (carry or target). Penalty-nullified
# plays are play_type "no_play" and drop out here; kneels, sacks and
# two-point tries aren't TD-scoring opportunities.
opps <- pbp |>
  filter(play_type %in% c("run", "pass"), !is.na(posteam), !is.na(yardline_100),
         coalesce(two_point_attempt, 0) == 0, coalesce(qb_kneel, 0) == 0) |>
  mutate(
    is_carry  = rush_attempt == 1 & !is.na(rusher_player_id),
    is_target = pass_attempt == 1 & coalesce(sack, 0) == 0 & !is.na(receiver_player_id),
    gsis_id   = case_when(is_carry ~ rusher_player_id, is_target ~ receiver_player_id),
    scored    = coalesce(touchdown, 0) == 1 & coalesce(td_team == posteam, FALSE) &
                coalesce(td_player_id == gsis_id, FALSE),
    ez_target = is_target & !is.na(air_yards) & (yardline_100 - air_yards) <= 0,
    opp_type  = if_else(is_carry, "carry", "target"),
    ydl_bucket = cut(yardline_100, c(0, 2, 5, 10, 20, 40, 100),
                     labels = c("1-2", "3-5", "6-10", "11-20", "21-40", "41+"))
  ) |>
  filter(is_carry | is_target) |>
  select(season, week, game_id, posteam, defteam, gsis_id, opp_type, yardline_100,
         ydl_bucket, ez_target, scored)

# Team-level drive outcomes, for red-zone trips and TD rate allowed. A trip
# is a drive with at least one snap inside the 20.
rz_drives <- pbp |>
  filter(!is.na(posteam), !is.na(fixed_drive), !is.na(yardline_100)) |>
  group_by(season, week, game_id, posteam, defteam, fixed_drive) |>
  summarise(reached_rz = any(yardline_100 <= 20),
            td = coalesce(first(fixed_drive_result) == "Touchdown", FALSE), .groups = "drop") |>
  filter(reached_rz)

# Team points per game (for the environment multiplier's baseline).
team_scores <- bind_rows(
  schedules |> transmute(season, week, team = home_team, pts = home_score),
  schedules |> transmute(season, week, team = away_team, pts = away_score)
) |> filter(!is.na(pts))

implied_totals <- bind_rows(
  schedules |> transmute(season, week, team = home_team, opponent = away_team, home = TRUE,
                         gameday, gametime, game_played = !is.na(result),
                         implied_total = (total_line + spread_line) / 2),
  schedules |> transmute(season, week, team = away_team, opponent = home_team, home = FALSE,
                         gameday, gametime, game_played = !is.na(result),
                         implied_total = (total_line - spread_line) / 2)
)

# --- League TD rates per opportunity type (prior seasons only) -------------
td_rate_table <- function(target_season) {
  hist <- opps |> filter(season < target_season)
  if (nrow(hist) == 0) stop("Need at least one prior season in the pbp cache for TD rates.")
  hist |>
    group_by(opp_type, ydl_bucket, ez_target) |>
    summarise(n = n(), td_rate = mean(scored), .groups = "drop")
}

pct <- function(x) 100 * percent_rank(x)

# --- Core: project one season/week -----------------------------------------
project_week <- function(target_season, target_week, slate_players) {
  rates <- td_rate_table(target_season)

  prior <- opps |>
    filter(season == target_season, week < target_week) |>
    left_join(rates |> select(opp_type, ydl_bucket, ez_target, td_rate),
              by = c("opp_type", "ydl_bucket", "ez_target")) |>
    mutate(
      xtd     = coalesce(td_rate, 0),
      recent  = week >= target_week - RECENT_WEEKS,
      i5_carry   = opp_type == "carry" & yardline_100 <= 5,
      i10_carry  = opp_type == "carry" & yardline_100 <= 10,
      i10_target = opp_type == "target" & yardline_100 <= 10,
      hl         = i10_carry | i10_target | ez_target,   # high-leverage look
      rz         = yardline_100 <= 20
    )
  if (nrow(prior) == 0) stop("No plays before season ", target_season, " week ", target_week, ".")

  # Team red-zone opportunities per game, to divide player RZ looks by.
  team_rz_game <- prior |>
    group_by(game_id, posteam) |>
    summarise(team_rz_opps = sum(rz), .groups = "drop")

  player_game <- prior |>
    group_by(season, week, game_id, posteam, gsis_id, recent) |>
    summarise(
      opps = n(), i5_carries = sum(i5_carry), i10_carries = sum(i10_carry),
      i10_targets = sum(i10_target), ez_targets = sum(ez_target),
      rz_opps = sum(rz), hl_xtd = sum(xtd[hl]), xtd = sum(xtd), tds = sum(scored),
      .groups = "drop"
    ) |>
    left_join(team_rz_game, by = c("game_id", "posteam"))

  summarise_window <- function(df, suffix) {
    df |>
      group_by(gsis_id) |>
      summarise(
        games = n_distinct(game_id),
        i5_carries_pg  = sum(i5_carries) / games,
        i10_carries_pg = sum(i10_carries) / games,
        i10_targets_pg = sum(i10_targets) / games,
        ez_targets_pg  = sum(ez_targets) / games,
        hl_xtd_pg      = sum(hl_xtd) / games,
        xtd_pg         = sum(xtd) / games,
        rz_share       = if_else(sum(team_rz_opps) > 0, sum(rz_opps) / sum(team_rz_opps), 0),
        tds            = sum(tds),
        .groups = "drop"
      ) |>
      rename_with(~ paste0(.x, suffix), -gsis_id)
  }

  season_stats <- summarise_window(player_game, "_szn")
  recent_stats <- summarise_window(player_game |> filter(recent), "_l5")

  blend <- function(recent, season) {
    if_else(is.na(recent), season, RECENT_WEIGHT * recent + (1 - RECENT_WEIGHT) * season)
  }

  # Team context: RZ trips/game (offense) and points/game, season-to-date and recent.
  team_trips <- rz_drives |>
    filter(season == target_season, week < target_week) |>
    group_by(team = posteam) |>
    summarise(rz_trips_pg_szn = n() / n_distinct(game_id),
              rz_trips_pg_l5 = sum(week >= target_week - RECENT_WEEKS) /
                pmax(n_distinct(game_id[week >= target_week - RECENT_WEEKS]), 1),
              .groups = "drop")

  team_ppg <- team_scores |>
    filter(season == target_season, week < target_week) |>
    group_by(team) |>
    summarise(ppg_szn = mean(pts), .groups = "drop")

  def_rz <- rz_drives |> filter(season == target_season, week < target_week)
  league_rz_td_rate <- mean(def_rz$td)
  opp_def <- def_rz |>
    group_by(opponent = defteam) |>
    summarise(opp_rz_trips_allowed = n(), opp_rz_tds_allowed = sum(td), .groups = "drop") |>
    mutate(
      opp_rz_td_rate_raw = opp_rz_tds_allowed / opp_rz_trips_allowed,
      opp_rz_td_rate = (opp_rz_tds_allowed + OPP_SHRINK_K * league_rz_td_rate) /
        (opp_rz_trips_allowed + OPP_SHRINK_K)
    )

  slate <- implied_totals |>
    filter(season == target_season, week == target_week) |>
    select(team, opponent, home, gameday, gametime, game_played, implied_total) |>
    left_join(team_trips, by = "team") |>
    left_join(team_ppg, by = "team") |>
    left_join(opp_def, by = "opponent") |>
    mutate(
      opp_rz_td_rate = coalesce(opp_rz_td_rate, league_rz_td_rate),
      # Composite environment input: implied total, else RZ trips/game.
      env_input = coalesce(implied_total, rz_trips_pg_szn * 3.5),
      env_pct = pct(env_input),
      opp_pct = pct(opp_rz_td_rate),
      env_mult = sqrt(pmin(pmax(coalesce(implied_total / ppg_szn, 1), 0.5), 2)),
      opp_mult = sqrt(opp_rz_td_rate / league_rz_td_rate)
    )

  slate_players |>
    inner_join(slate, by = "team") |>
    inner_join(season_stats, by = "gsis_id") |>
    left_join(recent_stats, by = "gsis_id") |>
    filter(games_szn >= MIN_GAMES) |>
    mutate(
      hl_xtd_pg = blend(hl_xtd_pg_l5, hl_xtd_pg_szn),
      rz_share  = blend(rz_share_l5, rz_share_szn),
      xtd_pg    = blend(xtd_pg_l5, xtd_pg_szn),
      hl_pct    = pct(hl_xtd_pg),
      share_pct = pct(rz_share),
      composite = round(W_HL * hl_pct + W_SHARE * share_pct + W_ENV * env_pct + W_OPP * opp_pct, 1),
      lambda    = xtd_pg * env_mult * opp_mult,
      td_prob   = 1 - exp(-lambda),
      season = target_season, week = target_week
    ) |>
    arrange(desc(td_prob))
}

# --- Historical replay + calibration -----------------------------------------
# Replays project_week() on past weeks (6-18, so season-to-date has some
# depth) and attaches whether each player actually scored.
# Historical slate = every QB/RB/WR/TE who got an opportunity THAT week
# (players who actually suited up and touched the ball), on the team they
# played for. That's slightly kinder than the live slate, which also
# includes active players who end up getting zero touches.
replay_season <- function(s) {
  weeks <- opps |> filter(season == s, week >= 6) |> distinct(week) |> pull(week) |> sort()
  purrr::map_dfr(weeks, function(w) {
    played <- opps |> filter(season == s, week == w)
    actual <- played |> group_by(gsis_id) |> summarise(scored_td = any(scored), .groups = "drop")
    project_week(s, w, played |> distinct(gsis_id, team = posteam)) |>
      inner_join(actual, by = "gsis_id") |>
      select(season, week, gsis_id, lambda, td_prob, composite, scored_td)
  })
}

# The raw Poisson probabilities rank well but come out over-spread: the
# 2025-26 replay put the top decile at 53% predicted / 45% actual and the
# bottom at 2% / 9%. Partly that's sample noise in a player's handful of
# games, partly fringe players' TDs that xTD can't see. Fix: logistic
# recalibration (Platt scaling) on log(lambda), fit ONLY on the previous
# season's replay, so a season is never calibrated on its own outcomes.
fit_calibration <- function(replay) {
  glm(scored_td ~ log(lambda + 0.01), family = binomial(), data = replay)
}
apply_calibration <- function(fit, lambda) {
  unname(predict(fit, newdata = data.frame(lambda = lambda), type = "response"))
}

# --- Backtest: how well do the probabilities and composite hold up? ---------
run_backtest <- function() {
  seasons_avail <- sort(unique(opps$season))
  test_seasons <- tail(seasons_avail, 3)  # each needs the prior season to calibrate on
  test_seasons <- test_seasons[test_seasons - 1 > min(seasons_avail)]
  log_msg("Replaying seasons ", min(test_seasons) - 1, "-", max(test_seasons),
          " (weeks 6+), calibrating each on the season before")
  replays <- purrr::map(setNames(nm = c(min(test_seasons) - 1, test_seasons)), replay_season)

  res <- purrr::map_dfr(test_seasons, function(s) {
    fit <- fit_calibration(replays[[as.character(s - 1)]])
    replays[[as.character(s)]] |> mutate(td_prob_cal = apply_calibration(fit, lambda))
  })

  base_rate <- mean(res$scored_td)
  brier_base <- mean((base_rate - res$scored_td)^2)
  cat(sprintf("\nBacktest (out-of-sample, %s): %d player-games, actual TD rate %.3f\n",
              paste(test_seasons, collapse = ", "), nrow(res), base_rate))
  for (col in c("td_prob", "td_prob_cal")) {
    b <- mean((res[[col]] - res$scored_td)^2)
    cat(sprintf("  %-12s mean %.3f  Brier %.4f  (flat base rate %.4f, skill %.1f%%)\n",
                if (col == "td_prob") "raw" else "calibrated", mean(res[[col]]), b, brier_base,
                100 * (1 - b / brier_base)))
  }
  cat("\nCalibration by predicted-probability decile (calibrated):\n")
  res |>
    mutate(bin = ntile(td_prob_cal, 10)) |>
    group_by(bin) |>
    summarise(n = n(), raw = round(mean(td_prob), 3), calibrated = round(mean(td_prob_cal), 3),
              actual = round(mean(scored_td), 3)) |>
    as.data.frame() |> print(row.names = FALSE)
  cat("\nBy season (calibrated):\n")
  res |> group_by(season) |>
    summarise(n = n(), predicted = round(mean(td_prob_cal), 3), actual = round(mean(scored_td), 3),
              brier = round(mean((td_prob_cal - scored_td)^2), 4)) |>
    as.data.frame() |> print(row.names = FALSE)
  cat("\nActual TD rate by composite-score quintile:\n")
  res |>
    mutate(quintile = ntile(composite, 5)) |>
    group_by(quintile) |>
    summarise(n = n(), mean_composite = round(mean(composite), 1), actual = round(mean(scored_td), 3)) |>
    as.data.frame() |> print(row.names = FALSE)
  invisible(res)
}

# --- Main -------------------------------------------------------------------
if (do_backtest) {
  run_backtest()
  quit(save = "no")
}

if (is.na(opt_season) || is.na(opt_week)) {
  upcoming <- schedules |> filter(is.na(result)) |> arrange(season, week)
  if (nrow(upcoming) == 0) stop("No unplayed games in the cached schedule — pass --season/--week.")
  opt_season <- upcoming$season[1]; opt_week <- upcoming$week[1]
}
log_msg("Projecting anytime TD scorers for season ", opt_season, " week ", opt_week)

# Active slate players: latest roster row per player this season, QB/RB/WR/TE,
# ACT or INA only (RES/DEV/CUT never appear on injury reports, see
# R/predict_upcoming_week.R), minus anyone ruled Out for this week.
rosters <- read_parquet(file.path(RAW_DIR, "rosters", "rosters.parquet"))
slate_players <- rosters |>
  filter(season == opt_season, position %in% POSITIONS, status %in% c("ACT", "INA")) |>
  group_by(gsis_id) |>
  slice_max(order_by = coalesce(week, 0), n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(gsis_id, player = full_name, position, team)

injury_files <- list.files(file.path(RAW_DIR, "injuries"), pattern = "\\.parquet$", full.names = TRUE)
injuries <- if (length(injury_files) == 0) tibble(gsis_id = character(), report_status = character()) else
  open_dataset(injury_files) |>
    filter(season == opt_season, week == opt_week) |>
    select(gsis_id, report_status) |> collect() |> distinct(gsis_id, .keep_all = TRUE)
log_msg(nrow(injuries), " injury report rows for this week (0 is normal before Wednesday)")

# Optional odds overlay: anytime-TD consensus line for this week, if a pull exists.
odds_path <- file.path(ODDS_DIR, sprintf("season=%d_week=%d.parquet", opt_season, opt_week))
odds <- if (file.exists(odds_path)) {
  o <- read_parquet(odds_path) |> filter(market == "player_anytime_td")
  if (!"yes_price" %in% names(o)) o$yes_price <- NA_real_
  if (!"pulled_at" %in% names(o)) o$pulled_at <- NA_character_
  o |> transmute(gsis_id, book_prob = fair_prob_over, book_devigged = devigged,
                 book_n = n_books, book_price = yes_price, odds_pulled_at = as.character(pulled_at)) |>
    distinct(gsis_id, .keep_all = TRUE)
} else {
  tibble(gsis_id = character(), book_prob = double(), book_devigged = logical(),
         book_n = integer(), book_price = double(), odds_pulled_at = character())
}
log_msg(nrow(odds), " anytime-TD lines found for this week",
        if (nrow(odds) == 0) " (no odds pull yet — projections only)" else "")

log_msg("Fitting probability calibration on the ", opt_season - 1, " replay")
calib <- fit_calibration(replay_season(opt_season - 1))

prob_to_american <- function(p) {
  ifelse(is.na(p), NA_real_, ifelse(p >= 0.5, -100 * p / (1 - p), 100 * (1 - p) / p))
}

proj <- project_week(opt_season, opt_week, slate_players) |>
  # A game that's already been played (e.g. Thursday night, on a Friday
  # run) is no longer bettable; leave its players off.
  filter(!game_played) |>
  mutate(td_prob_raw = td_prob, td_prob = apply_calibration(calib, lambda)) |>
  arrange(desc(td_prob)) |>
  left_join(injuries, by = "gsis_id") |>
  mutate(injury_status = coalesce(report_status, "Healthy")) |>
  filter(injury_status != "Out") |>
  left_join(odds, by = "gsis_id") |>
  mutate(
    fair_price = round(prob_to_american(td_prob)),
    book_price = coalesce(book_price, round(prob_to_american(book_prob))),
    edge = td_prob - book_prob
  ) |>
  select(season, week, player, position, team, opponent, home, gameday, gametime, injury_status,
         td_prob, td_prob_raw, fair_price, book_price, book_prob, book_devigged, book_n, edge,
         odds_pulled_at, composite, hl_pct, share_pct, env_pct, opp_pct,
         games_szn, games_l5,
         i5_carries_pg_l5, i10_carries_pg_l5, i10_targets_pg_l5, ez_targets_pg_l5,
         i5_carries_pg_szn, i10_carries_pg_szn, i10_targets_pg_szn, ez_targets_pg_szn,
         hl_xtd_pg, rz_share, rz_share_l5, rz_share_szn, xtd_pg, tds_szn,
         implied_total, ppg_szn, rz_trips_pg_l5, rz_trips_pg_szn,
         opp_rz_td_rate, opp_rz_td_rate_raw, opp_rz_trips_allowed,
         env_mult, opp_mult, lambda, gsis_id)

dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)
out_path <- file.path(OUT_DIR, sprintf("season=%d_week=%d.csv", opt_season, opt_week))
write.csv(proj |> mutate(across(where(is.numeric), ~ round(.x, 4))), out_path, row.names = FALSE)

cat(sprintf("\nAnytime TD projections — %d week %d — %d players (%s)\n\n",
            opt_season, opt_week, nrow(proj),
            paste(names(table(proj$position)), table(proj$position), sep = " ", collapse = ", ")))
proj |>
  slice_head(n = 30) |>
  transmute(player, pos = position, team, opp = opponent,
            `TD%` = sprintf("%.0f%%", 100 * td_prob), score = composite,
            book = if_else(is.na(book_prob), "", sprintf("%.0f%%", 100 * book_prob)),
            `i10/g` = round(coalesce(i10_carries_pg_l5, 0) + coalesce(i10_targets_pg_l5, 0), 1),
            `EZt/g` = round(coalesce(ez_targets_pg_l5, 0), 1),
            `RZ%` = sprintf("%.0f%%", 100 * rz_share),
            imp = implied_total, `oppRZ%` = sprintf("%.0f%%", 100 * opp_rz_td_rate),
            inj = if_else(injury_status == "Healthy", "", injury_status)) |>
  as.data.frame() |> print(row.names = FALSE)
log_msg("Wrote ", out_path)
