# Download everything the TD scorer reads, straight from nflverse, into
# data/raw/ in the same layout as the local NFL MODEL ingestion. The cloud
# runner starts empty every time, so this runs at the start of every update.
# data/raw/ is git-ignored (~150 MB of play-by-play); it's re-downloaded
# each run.

suppressMessages({
  library(dplyr)
  library(arrow)
})
log_msg <- function(...) message(format(Sys.time(), "[%Y-%m-%d %H:%M:%S] "), ...)

RAW_DIR <- Sys.getenv("TD_RAW_DIR", "data/raw")
FIRST_SEASON <- 2019  # same history as the local model: league TD rates use every prior season
current <- nflreadr::most_recent_season()
seasons <- FIRST_SEASON:current

for (d in c("pbp", "schedules", "rosters", "injuries")) {
  dir.create(file.path(RAW_DIR, d), recursive = TRUE, showWarnings = FALSE)
}

log_msg("Play-by-play ", FIRST_SEASON, "-", current)
pbp <- nflreadr::load_pbp(seasons)
for (s in seasons) {
  write_parquet(filter(pbp, season == s), file.path(RAW_DIR, "pbp", paste0("season=", s, ".parquet")))
}
log_msg("  ", nrow(pbp), " plays; ", n_distinct(pbp$game_id[pbp$season == current]),
        " games so far in ", current)
rm(pbp); invisible(gc())

log_msg("Schedules")
write_parquet(nflreadr::load_schedules(seasons), file.path(RAW_DIR, "schedules", "schedules.parquet"))

log_msg("Rosters (", current, ") + player ID crosswalk")
write_parquet(nflreadr::load_rosters(current), file.path(RAW_DIR, "rosters", "rosters.parquet"))
write_parquet(nflreadr::load_ff_playerids(), file.path(RAW_DIR, "rosters", "player_ids_crosswalk.parquet"))

log_msg("Injury reports (", current, ")")
inj <- tryCatch(nflreadr::load_injuries(current), error = function(e) NULL)
if (is.null(inj) || nrow(inj) == 0) {
  log_msg("  no injury data for ", current, " yet; everyone will show as Healthy")
} else {
  write_parquet(inj, file.path(RAW_DIR, "injuries", paste0("season=", current, ".parquet")))
}

log_msg("Data fetch complete")
