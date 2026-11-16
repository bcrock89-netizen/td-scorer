# Build docs/index.html from every week's projection CSV in docs/weeks/.
# All weeks are embedded in the page (a few hundred KB), so the week
# dropdown works with no server. The page itself is site/template.html;
# edit that for design changes.

suppressMessages({
  library(dplyr)
  library(jsonlite)
})
log_msg <- function(...) message(format(Sys.time(), "[%Y-%m-%d %H:%M:%S] "), ...)

WEEKS_DIR <- Sys.getenv("TD_OUT_DIR", "docs/weeks")
files <- list.files(WEEKS_DIR, pattern = "^season=\\d+_week=\\d+\\.csv$", full.names = TRUE)
if (length(files) == 0) stop("No projection CSVs in ", WEEKS_DIR, " — run R/td_scorer_model.R first.")

keep_cols <- c("player", "position", "team", "opponent", "home", "gameday", "gametime",
               "injury_status", "td_prob", "fair_price", "book_price", "book_prob",
               "book_devigged", "book_n", "edge", "odds_pulled_at", "composite",
               "hl_pct", "share_pct", "env_pct", "opp_pct", "games_l5", "games_szn",
               "i5_carries_pg_l5", "i10_carries_pg_l5", "i10_targets_pg_l5", "ez_targets_pg_l5",
               "i5_carries_pg_szn", "i10_carries_pg_szn", "i10_targets_pg_szn", "ez_targets_pg_szn",
               "rz_share", "rz_share_l5", "rz_share_szn", "tds_szn", "implied_total",
               "rz_trips_pg_l5", "opp_rz_td_rate")

weeks <- lapply(files, function(f) {
  d <- read.csv(f, stringsAsFactors = FALSE)
  d <- d[, intersect(keep_cols, names(d)), drop = FALSE]
  list(
    season = as.integer(sub(".*season=(\\d+)_.*", "\\1", f)),
    week = as.integer(sub(".*_week=(\\d+)\\.csv$", "\\1", f)),
    built_at = format(file.mtime(f), "%a %b %d, %I:%M %p %Z", tz = "America/New_York"),
    csv = file.path("weeks", basename(f)),
    rows = d
  )
})
weeks <- weeks[order(sapply(weeks, function(w) w$season * 100 + w$week), decreasing = TRUE)]
names(weeks) <- sapply(weeks, function(w) sprintf("%d-%02d", w$season, w$week))

payload <- toJSON(weeks, dataframe = "rows", auto_unbox = TRUE, digits = 4, na = "null")
template <- paste(readLines("site/template.html", warn = FALSE, encoding = "UTF-8"), collapse = "\n")
# Keep "</" out of the inline JSON so a value can never close the <script> tag.
html <- sub("__TD_DATA__", gsub("</", "<\\\\/", payload, fixed = TRUE), template, fixed = TRUE)

dir.create("docs", showWarnings = FALSE)
writeLines(html, "docs/index.html", useBytes = TRUE)
log_msg("Built docs/index.html (", length(weeks), " week(s); latest ", names(weeks)[1], ", ",
        nrow(weeks[[1]]$rows), " players)")
