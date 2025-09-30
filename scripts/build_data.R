# scripts/build_data.R
suppressPackageStartupMessages({
  library(nflreadr)
  library(dplyr)
  library(readr)
  library(jsonlite)
  library(lubridate)
  library(stringr)
})

SEASON <- 2025L
OUT_DIR <- "public"
ROSTER_WHITELIST <- "whitelist/players.csv"  # optional; if absent => Top-200 filter

dir.create(file.path(OUT_DIR, paste0("s", SEASON), "season"), recursive = TRUE, showWarnings = FALSE)

# Find the latest completed regular-season week
sched <- nflreadr::load_schedules(SEASON)
latest_week <- sched |>
  dplyr::filter(game_type == "REG", !is.na(result)) |>
  dplyr::summarise(latest = max(week, na.rm = TRUE)) |>
  dplyr::pull(latest)
if (is.infinite(latest_week) || is.na(latest_week)) latest_week <- 1L

# Load season-to-date and weekly stats from nflverse
season_dt <- nflreadr::load_player_stats(seasons = SEASON, stat_type = "regpost")
week_dt   <- nflreadr::load_player_stats(seasons = SEASON, stat_type = "week") |>
  dplyr::filter(week == latest_week)

# Keep compact, fantasy-useful columns
keep_cols <- c(
  "player_id","player_name","position","recent_team",
  "week","opponent_team",
  "offense_snaps","routes_run","targets","receptions",
  "rushing_attempts","rushing_yards","rushing_tds",
  "receiving_yards","receiving_tds",
  "passing_yards","passing_tds","interceptions",
  "fumbles_lost",
  "fantasy_points_ppr","fantasy_points"
)

rename_map <- c(recent_team = "team", opponent_team = "opponent", offense_snaps = "snaps")

clean_cols <- function(df, include_week = TRUE) {
  cols <- intersect(keep_cols, names(df))
  df <- df |> dplyr::select(dplyr::all_of(cols))
  for (nm in names(rename_map)) {
    if (nm %in% names(df)) names(df)[names(df) == nm] <- rename_map[[nm]]
  }
  if (!include_week && "week" %in% names(df)) df <- df |> dplyr::select(-week)
  df
}

season_clean <- clean_cols(season_dt, include_week = FALSE)
week_clean   <- clean_cols(week_dt,   include_week = TRUE)

# Apply whitelist OR Top-200 by PPR
if (file.exists(ROSTER_WHITELIST)) {
  wl <- readr::read_csv(ROSTER_WHITELIST, show_col_types = FALSE) |>
    dplyr::rename_with(~tolower(.x)) |>
    dplyr::mutate(player_name = dplyr::coalesce(player_name, name),
                  player_id   = dplyr::coalesce(player_id, gsis_id, id))

  wl_names <- wl |> dplyr::filter(!is.na(player_name)) |> dplyr::pull(player_name) |> unique()
  wl_ids   <- wl |> dplyr::filter(!is.na(player_id))   |> dplyr::pull(player_id)   |> unique()

  season_clean <- season_clean |>
    dplyr::filter((!is.na(player_name) & player_name %in% wl_names) |
                  (!is.na(player_id)  & player_id  %in% wl_ids))

  week_clean <- week_clean |>
    dplyr::filter((!is.na(player_name) & player_name %in% wl_names) |
                  (!is.na(player_id)  & player_id  %in% wl_ids))
} else {
  season_clean <- season_clean |> dplyr::arrange(dplyr::desc(fantasy_points_ppr)) |> dplyr::slice_head(n = 200)
  week_clean   <- week_clean   |> dplyr::arrange(dplyr::desc(fantasy_points_ppr)) |> dplyr::slice_head(n = 200)
}

# Write season files
season_dir <- file.path(OUT_DIR, paste0("s", SEASON), "season")
readr::write_csv(season_clean, file.path(season_dir, "players_min.csv"))
jsonlite::write_json(season_clean, file.path(season_dir, "players_min.json"),
                     dataframe = "rows", auto_unbox = TRUE, pretty = FALSE)
jsonlite::write_json(list(season = SEASON, generated_at = as.character(lubridate::with_tz(Sys.time(), "UTC"))),
                     file.path(season_dir, "meta.json"), auto_unbox = TRUE, pretty = TRUE)

# Write latest week files + snapshot folder
wdir <- file.path(OUT_DIR, paste0("s", SEASON), sprintf("w%02d", latest_week))
dir.create(wdir, recursive = TRUE, showWarnings = FALSE)
readr::write_csv(week_clean, file.path(wdir, "players_week_min.csv"))
jsonlite::write_json(week_clean, file.path(wdir, "players_week_min.json"),
                     dataframe = "rows", auto_unbox = TRUE, pretty = FALSE)
jsonlite::write_json(list(season = SEASON, week = latest_week, generated_at = as.character(lubridate::with_tz(Sys.time(), "UTC"))),
                     file.path(wdir, "meta.json"), auto_unbox = TRUE, pretty = TRUE)

# Stable "latest" copy
latest_dir <- file.path(OUT_DIR, paste0("s", SEASON), "latest")
dir.create(latest_dir, recursive = TRUE, showWarnings = FALSE)
file.copy(file.path(wdir, "players_week_min.csv"),  file.path(latest_dir, "players_week_min.csv"), overwrite = TRUE)
file.copy(file.path(wdir, "players_week_min.json"), file.path(latest_dir, "players_week_min.json"), overwrite = TRUE)
file.copy(file.path(wdir, "meta.json"),             file.path(latest_dir, "meta.json"), overwrite = TRUE)
