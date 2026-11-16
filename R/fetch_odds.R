# Pull ANYTIME TD lines (one market only) for the upcoming week from The
# Odds API and merge them into data/odds/season=X_week=Y.parquet.
#
# Credits: The Odds API charges per event call, per market, per region.
# This pulls 1 market x 1 region ("us"), so roughly 1 credit per game on
# the slate (~13-16 per pull). The free tier's 500/month is shared with
# the local WR pipeline's pulls (scripts/09_run_odds.R), so keep an eye
# on the "credits remaining" line this prints.
#
# Same approach as the local R/01_ingest_odds.R, cut down to one market:
# - Anytime TD is usually "Yes"-only. When a book also posts "No", the
#   pair is devigged with the power method; otherwise the Yes price is
#   kept as-is (devigged = FALSE) and still includes the book's margin.
# - Consensus across books: median Yes price, mean probability.
# - Re-pulls MERGE with the week's earlier file (fresh lines win, lines no
#   longer listed are kept), since books drop games once they kick off.
# - No key, or no lines posted yet: logs a message and exits cleanly, so
#   the site still updates with projections only.

suppressMessages({
  library(dplyr)
  library(arrow)
  library(httr)
  library(jsonlite)
})
log_msg <- function(...) message(format(Sys.time(), "[%Y-%m-%d %H:%M:%S] "), ...)

RAW_DIR  <- Sys.getenv("TD_RAW_DIR", "data/raw")
ODDS_DIR <- Sys.getenv("TD_ODDS_DIR", "data/odds")
api_key  <- Sys.getenv("THE_ODDS_API_KEY")

american_odds_to_prob <- function(price) ifelse(price > 0, 100 / (price + 100), -price / (-price + 100))
devig_power <- function(p_raw) {
  if (sum(p_raw) <= 1) return(p_raw)
  k <- tryCatch(uniroot(function(k) sum(p_raw^k) - 1, c(0.01, 20))$root, error = function(e) NA_real_)
  if (is.na(k)) p_raw / sum(p_raw) else p_raw^k
}
normalize_name <- function(x) {
  x <- gsub("\\s+(Jr|Sr)\\.?$", " \\1.", x, ignore.case = TRUE)
  tolower(nflreadr::clean_player_names(x))
}

if (identical(api_key, "")) {
  log_msg("THE_ODDS_API_KEY not set — skipping odds pull (site will show projections only).")
  quit(save = "no", status = 0)
}

schedules <- read_parquet(file.path(RAW_DIR, "schedules", "schedules.parquet")) |>
  filter(game_type == "REG")
upcoming <- schedules |> filter(is.na(result)) |> arrange(season, week)
if (nrow(upcoming) == 0) { log_msg("No unplayed games — nothing to price."); quit(save = "no") }
target_season <- upcoming$season[1]; target_week <- upcoming$week[1]

teams <- nflreadr::load_teams(current = TRUE) |> select(team_abbr, team_name)
week_games <- schedules |>
  filter(season == target_season, week == target_week, is.na(result)) |>
  select(home_team, away_team) |>
  left_join(teams, by = c("home_team" = "team_abbr")) |> rename(home_name = team_name) |>
  left_join(teams, by = c("away_team" = "team_abbr")) |> rename(away_name = team_name)

SPORT <- "americanfootball_nfl"
events_resp <- GET(sprintf("https://api.the-odds-api.com/v4/sports/%s/events", SPORT),
                   query = list(apiKey = api_key))
if (http_error(events_resp)) {
  log_msg("Events call failed (HTTP ", status_code(events_resp), ") — skipping odds.")
  quit(save = "no", status = 0)
}
events <- fromJSON(content(events_resp, "text", encoding = "UTF-8"), simplifyVector = FALSE)
matched <- Filter(function(e) any(week_games$home_name == e$home_team & week_games$away_name == e$away_team), events)
log_msg(length(matched), " of ", nrow(week_games), " unplayed week-", target_week, " games found at The Odds API")
if (length(matched) == 0) { log_msg("No matching events yet — skipping odds."); quit(save = "no") }

rows <- list(); last_resp <- NULL
for (ev in matched) {
  resp <- GET(sprintf("https://api.the-odds-api.com/v4/sports/%s/events/%s/odds", SPORT, ev$id),
              query = list(apiKey = api_key, regions = "us", markets = "player_anytime_td",
                           oddsFormat = "american"))
  last_resp <- resp
  if (http_error(resp)) {
    log_msg("  ", ev$away_team, " @ ", ev$home_team, ": HTTP ", status_code(resp), ", skipped")
    next
  }
  parsed <- fromJSON(content(resp, "text", encoding = "UTF-8"), simplifyVector = FALSE)
  for (bm in parsed$bookmakers) for (mkt in bm$markets) for (oc in mkt$outcomes) {
    rows[[length(rows) + 1]] <- tibble(
      bookmaker = bm$key,
      player_desc = if (!is.null(oc$description)) oc$description else NA_character_,
      side = oc$name, price = as.numeric(oc$price))
  }
  Sys.sleep(0.3)
}
if (!is.null(last_resp)) {
  rem <- headers(last_resp)[["x-requests-remaining"]]
  if (!is.null(rem)) log_msg("The Odds API credits remaining this month: ", rem)
}

long <- bind_rows(rows)
if (nrow(long) == 0) { log_msg("No anytime-TD prices posted yet — skipping odds."); quit(save = "no") }

per_book <- long |>
  filter(!is.na(player_desc)) |>
  group_by(bookmaker, player_desc) |>
  summarise(yes = price[side == "Yes"][1], no = price[side == "No"][1], .groups = "drop") |>
  filter(!is.na(yes)) |>
  rowwise() |>
  mutate(devigged = !is.na(no),
         prob = if (devigged) devig_power(c(american_odds_to_prob(yes), american_odds_to_prob(no)))[1]
                else american_odds_to_prob(yes)) |>
  ungroup()

crosswalk <- read_parquet(file.path(RAW_DIR, "rosters", "player_ids_crosswalk.parquet")) |>
  filter(!is.na(gsis_id), !is.na(merge_name)) |>
  distinct(merge_name, .keep_all = TRUE) |>
  select(gsis_id, match_name = merge_name)

consensus <- per_book |>
  group_by(player_desc) |>
  summarise(yes_price = median(yes), fair_prob_over = mean(prob),
            devigged = any(devigged), n_books = n(), .groups = "drop") |>
  mutate(match_name = normalize_name(player_desc)) |>
  left_join(crosswalk, by = "match_name")

unmatched <- consensus |> filter(is.na(gsis_id))
if (nrow(unmatched) > 0) log_msg(nrow(unmatched), " name(s) not matched (dropped): ",
                                 paste(unmatched$player_desc, collapse = ", "))

fresh <- consensus |>
  filter(!is.na(gsis_id)) |>
  transmute(gsis_id, player_desc, market = "player_anytime_td", point = 0.5, yes_price,
            fair_prob_over, devigged, n_books, season = target_season, week = target_week,
            pulled_at = format(Sys.time(), "%Y-%m-%d %H:%M %Z", tz = "America/New_York"))

dir.create(ODDS_DIR, recursive = TRUE, showWarnings = FALSE)
out_path <- file.path(ODDS_DIR, sprintf("season=%d_week=%d.parquet", target_season, target_week))
if (file.exists(out_path)) {
  kept <- anti_join(read_parquet(out_path), fresh, by = c("gsis_id", "market"))
  log_msg("Merging: ", nrow(fresh), " fresh lines + ", nrow(kept), " kept from the earlier pull")
  fresh <- bind_rows(fresh, kept)
}
write_parquet(fresh, out_path)
log_msg("Wrote ", nrow(fresh), " anytime-TD lines -> ", out_path)
