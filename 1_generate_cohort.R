### Nick Cardamone
### OCC_PGHDPred
### 1. Generate Cohort
### Date created: 4/29/2025
### Last updated: 4/22/2026
# ---------------------------------------------------------------------------
# Extract all individuals with any PGHD data from DOEx.GENERIC PGHD in the following categories, "Sleep", "Workout", and "Daily Activity Summary".
# Define the analysis period as 6/01/2023 to 4/22/2026 and extract all outcomes data to the end of the latest possible observation period as 10/24/2025.
# Observation periods extend out a month after the last upload date for a given person.
# Extract and clean the names of 48 PGHD features. Also, we are expanding the data set so that all days in the analysis period are listed for every person - this will help when we paste in the outcome variables (ED, inpatient, or death) later.
# Finally, we create person-level summary data which has the first date of upload, and dates one year, two years, and five years before (adjusted for leap days) which we will use in subsequent code to extract prior demographic, vital, and health care utilization features.
# ---------------------------------------------------------------------------

# Function to install packages if not already installed
install_if_missing <- function(packages) {
  new_packages <- packages[!(packages %in% installed.packages()[, "Package"])]
  if (length(new_packages)) {
    install.packages(new_packages, dependencies = TRUE)
  }
}

# List of required packages
required_packages <- c(
  "DBI",# Working with data in databases
  "dbplyr", # Working with data in databases
  "dplyr",
  "data.table",
  "matrixStats",
  "stringr", # string var manipulation
  "arrow",  # parquet files
  "tidyverse", # helper functions
  "lubridate",
  "janitor",
  "readxl",
  "openxlsx",
  "future",
  "tictoc",
  "parquetize",
  "table1",
  "odbc",
  "traumar" # meterological season
)

# Install missing packages
install_if_missing(required_packages)

suppressPackageStartupMessages({
  library(DBI) # Working with data in databases
  library(dbplyr) # Working with data in databases
  library(dplyr)
  library(data.table)
  library(matrixStats)
  library(stringr) # string var manipulation
  library(arrow) #parquet files
  library(tidyverse) # helper functions
  library(lubridate)
  library(janitor)
  library(readxl)
  library(openxlsx)
  library(future)
  library(tictoc)
  library(parquetize)
  library(table1)
  library(odbc)
  library(traumar) # meterological season
})

'%!in%' <- function(x, y)
  ! ('%in%'(x, y))

# ---------------------------------------------------------------------------
# Set working directory
# ---------------------------------------------------------------------------
setwd(
  "C://Users//VHAPHICardaN//OneDrive - Department of Veterans Affairs//Desktop//Projects//OPS_Bressman-PGHDPred//pghdpred_deliverable"
)

# Connect to DB using ODBC with RB03 profile
con <- dbConnect(odbc::odbc(),
                 .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;",
                 timeout = 10)

# Connect to specific databases using ODBC
cdwwork_con <- dbConnect(
  odbc::odbc(),
  .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;",
  timeout = 10,
  database = "CDWWork"
)

db_pghpred_con <- dbConnect(
  odbc::odbc(),
  .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;",
  timeout = 10,
  database = "OCC_PGHDPred"
)

db_doex_con <- dbConnect(
  odbc::odbc(),
  .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;",
  database = "OIA_PGD",
  timeout = 10
)

# ---------------------------------------------------------------------------
# 1) Extract all individuals with any PGHD data from DOEx.GENERIC PGHD in the
#    following categories: "Sleep", "Workout", and "Daily Activity Summary".
#
# INCREMENTAL PULL STRATEGY
# ------------------------------------------------------------------
# Each calendar month is pulled once and saved as its own parquet file
# in data/pghd/PGHD_YYYY-MM.parquet.  On subsequent runs, months that
# already have a file are skipped automatically — only new or missing
# months are queried.  Set force_refresh <- TRUE to re-pull everything.
#
# WHY THE PULL STARTS IN 2022-01-01 (permanent fix)
# ------------------------------------------------------------------
# The analysis window for Study 1 requires that date_first be the patient's
# TRUE first upload date.  When the pull started on 2023-06-01, any patient
# who was already uploading before that date appeared to have date_first =
# 2023-06-01 — a left-truncation artefact.  Extending the pull to 2022-01-01
# (18 months of look-back) ensures those patients are identified as prevalent
# uploaders and correctly excluded by the date_first >= 2023-09-01 filter
# applied in model_analysis_writeup.Rmd (cohort_static and feature-engineering
# chunks).  The 2023-09-01 threshold is a 90-day grace period above the
# original 2023-06-01 window open to absorb any residual uncertainty.
# ---------------------------------------------------------------------------

force_refresh   <- FALSE
pghd_dir        <- 'data/input/pghd'
analysis_start  <- as.Date("2022-01-01")   # extended for accurate date_first (was 2023-06-01)
analysis_end    <- as.Date("2026-05-14")
allowed_devices <- c("apple_health", "fitbit", "garmin")

pghd_pull_chunk <- function(date_from, date_to) {
  # Pull and aggregate a date-bounded slice from DOEx.GENERIC_PGHD.
  # Filters are pushed to SQL before GROUP BY for sargable execution.
  # ObservationPID and units are not needed downstream and are excluded
  # to reduce transfer volume.
  tbl(db_doex_con, in_schema('DOEx', 'GENERIC_PGHD')) %>%
    filter(
      measurementDate >= !!as.character(date_from),
      measurementDate <= !!as.character(date_to),
      category == "Daily Activity Summary",
      device %in% !!allowed_devices,
      value >= 0
    ) %>%
    transmute(
      PatientICN  = ICN,
      date        = as.Date(measurementDate),
      category,
      measurement,
      device,
      value
    ) %>%
    group_by(PatientICN, date, category, measurement, device) %>%
    dplyr::summarize(
      n_obs = n(),
      val   = max(value, na.rm = TRUE)  # two uploads same day -> take max
    ) %>%
    ungroup() %>%
    collect()
}

if (!dir.exists("data"))    dir.create("data")
if (!dir.exists(pghd_dir))  dir.create(pghd_dir, recursive = TRUE)

# ---- Pull each month — skip months that already have a parquet file --------
month_starts <- seq(floor_date(analysis_start, "month"),
                    floor_date(analysis_end,   "month"),
                    by = "month")
n_months <- length(month_starts)

cat(sprintf("Checking %d monthly PGHD chunks in %s...\n", n_months, pghd_dir))

for (i in seq_along(month_starts)) {
  m_from    <- month_starts[[i]]
  m_to      <- min(m_from %m+% months(1) - days(1), analysis_end)
  month_lbl <- format(m_from, "%Y-%m")
  out_path  <- file.path(pghd_dir, sprintf("PGHD_%s.parquet", month_lbl))
  
  if (file.exists(out_path) && !force_refresh) {
    cat(sprintf("  [%d/%d] %s — already exists, skipping.\n", i, n_months, month_lbl))
    next
  }
  
  tic(sprintf("  [%d/%d] %s", i, n_months, month_lbl))
  chunk <- pghd_pull_chunk(date_from = m_from, date_to = m_to)
  toc()
  cat(sprintf("         \u2192 %s rows\n", format(nrow(chunk), big.mark = ",")))
  write_parquet(chunk, out_path)
}

# ---- Collect raw monthly parquet paths (cleaning happens in section 2) -----
pghd_files <- sort(list.files(pghd_dir, pattern = ".parquet$", full.names = TRUE))

if (length(pghd_files) == 0L) stop("No monthly parquet files found in ", pghd_dir)

# ---------------------------------------------------------------------------
# 2) Clean and remap in monthly chunks — skip months already cleaned.
#    One month is loaded at a time; rm() + gc() after each write keeps peak
#    RAM to roughly one month of data rather than the full multi-year pull.
# ---------------------------------------------------------------------------

# ── Lookup tables (defined once, reused per chunk) ─────────────────────────
meas_lookup <- c(
  # Daily Activity Summary
  "Daily Activity Summary|Average Heart Rate for Daily Summary" = "das_avgHR_bpm",
  "Daily Activity Summary|Max Heart Rate Measured"              = "das_maxHR_bpm",
  "Daily Activity Summary|Minimum Heart Rate Measured"          = "das_minHR_bpm",
  "Daily Activity Summary|Heart Rate Zone Very Low"             = "das_HRVeyLow_sec",
  "Daily Activity Summary|Heart Rate Zone Low"                  = "das_HRLow_sec",
  "Daily Activity Summary|Heart Rate Zone Medium"               = "das_HRMedium_sec",
  "Daily Activity Summary|Heart Rate Zone High"                 = "das_HRHigh_sec",
  "Daily Activity Summary|Energy Burned (Calories)"             = "das_burnedenergy_kcal",
  "Daily Activity Summary|Exercise duration"                    = "das_exercise_sec",
  "Daily Activity Summary|Exercise distance in 24 hour"         = "das_exercise_m",
  "Daily Activity Summary|Number of steps in 24 hour Measured"  = "das_exercise_steps",
  "Daily Activity Summary|Time Spent Fairly Active"             = "das_FairlyActive_sec",
  "Daily Activity Summary|Time Spent Lightly Active"            = "das_LightlyActive_sec",
  "Daily Activity Summary|Time Spent Meditating"                = "das_Meditating_sec",
  "Daily Activity Summary|Time Spent Very Active"               = "das_VeryActive_sec",
  # Workout
  "Workout|Max Heart Rate Measured"                             = "wo_maxHR_bpm",
  "Workout|Minimum Heart Rate Measured"                         = "wo_minHR_bpm",
  "Workout|Heart Rate Zone Very Low"                            = "wo_HRVeyLow_sec",
  "Workout|Heart Rate Zone Low"                                 = "wo_HRLow_sec",
  "Workout|Heart Rate Zone Medium"                              = "wo_HRMedium_sec",
  "Workout|Heart Rate Zone High"                                = "wo_HRHigh_sec",
  "Workout|Energy Burned (Calories)"                            = "wo_burnedenergy_kcal",
  "Workout|Number of steps in 24 hour Measured"                 = "wo_exercise_steps",
  "Workout|Exercise distance in 24 hour"                        = "wo_exercise_m",
  "Workout|Exercise duration"                                   = "wo_exercise_sec",
  "Workout|Time Spent Fairly Active"                            = "wo_FairlyActive_sec",
  "Workout|Time Spent Lightly Active"                           = "wo_LightlyActive_sec",
  "Workout|Time Spent Very Active"                              = "wo_VeryActive_sec"
)

meas_any_lookup <- c(
  "Heart Rate Variability"                    = "das_HRV_bpm",
  "Heart rate resting"                        = "das_HRResting_bpm",
  "Basal metabolic rate index"                = "das_BMRI_kcal",
  "Calories burned during activity"           = "das_burnedactivity_kcal",
  "Elevation climbed [Length/Time] 24 hour"   = "das_climbed_m",
  "Flights climbed 24 hour"                   = "das_exercise_flights",
  "Average Cadence"                           = "wo_acadence_rmin",
  "Average Speed"                             = "wo_avgspeed_ms",
  "Maximum Speed Reached"                     = "wo_maxspeed_ms",
  "Average Heart Rate for Workout"            = "wo_avgHR_bpm",
  "Time in REM Sleep"                         = "sleep_REM_sec",
  "In Bed Duration"                           = "sleep_BED_sec",
  "Deep Sleep Duration"                       = "sleep_DEEP_sec",
  "Light Sleep Duration"                      = "sleep_LIGHT_sec",
  "Time Spent Awake"                          = "sleep_AWAKE_sec",
  "Total Sleep Duration"                      = "sleep_TOTAL_sec",
  "Times Awakened"                            = "sleep_awakened_count",
  "Sleep Score (Overall Quality)"             = "sleep_quality_score",
  "Times Spent Restless"                      = "sleep_RESTLESS_sec",
  "Time Taken to Fall Asleep"                 = "sleep_FALLASLEEP_sec"
)

cleaned_dir  <- "data/input/pghd_cleaned"
date_ceiling <- as.Date("2026-04-14")

if (!dir.exists(cleaned_dir)) dir.create(cleaned_dir, recursive = TRUE)

cat(sprintf("Cleaning %d monthly PGHD chunks into %s...\n", length(pghd_files), cleaned_dir))

for (i in seq_along(pghd_files)) {
  month_lbl <- sub(".*PGHD_(\\d{4}-\\d{2})\\.parquet$", "\\1", pghd_files[[i]])
  out_path  <- file.path(cleaned_dir, sprintf("PGHD_cleaned_%s.parquet", month_lbl))

  if (file.exists(out_path) && !force_refresh) {
    cat(sprintf("  [%d/%d] %s — already cleaned, skipping.\n", i, length(pghd_files), month_lbl))
    next
  }

  tic(sprintf("  [%d/%d] %s", i, length(pghd_files), month_lbl))
  chunk <- open_dataset(pghd_files[[i]]) %>%
    select(-n_obs) %>%
    filter(date <= date_ceiling) %>%
    collect() %>%
    setDT()

  if (nrow(chunk) == 0L) { toc(); next }

  chunk[, cat_meas := paste0(category, "|", measurement)]
  chunk[cat_meas %in% names(meas_lookup), measurement := meas_lookup[cat_meas]]
  chunk[measurement %in% names(meas_any_lookup) & !cat_meas %in% names(meas_lookup),
        measurement := meas_any_lookup[measurement]]
  chunk[!cat_meas %in% names(meas_lookup) &
          !measurement %in% names(meas_any_lookup) &
          !measurement %in% unname(c(meas_lookup, meas_any_lookup)),
        measurement := NA_character_]
  chunk[, cat_meas := NULL]

  chunk[device == "apple_health", device := "ah"]
  chunk[device == "fitbit",       device := "fit"]
  chunk[device == "garmin",       device := "gar"]
  chunk[, val := round(val, 2)]

  pghd_final_chunk <- chunk[
    !is.na(measurement),
    .(PatientICN, date, device, measurement, val)
  ]
  write_parquet(pghd_final_chunk, out_path)
  toc()
  cat(sprintf("         → %s rows retained\n", format(nrow(pghd_final_chunk), big.mark = ",")))
  rm(chunk, pghd_final_chunk); gc()
}

# ── Read all cleaned chunks into pghd_final ────────────────────────────────
cleaned_files <- sort(list.files(cleaned_dir, pattern = ".parquet$", full.names = TRUE))
cat(sprintf("Reading %d cleaned chunks...\n", length(cleaned_files)))
pghd_final <- open_dataset(cleaned_files) %>%
  filter(measurement %in% c("das_exercise_steps", "das_exercise_m")) %>% collect()

pghd_final <- pghd_final %>% setDT()
         
         
cat(sprintf("pghd_final ready: %s rows, %s patients.\n",
            format(nrow(pghd_final), big.mark = ","),
            format(uniqueN(pghd_final$PatientICN), big.mark = ",")))

# ---------------------------------------------------------------------------
# 3) Pivot so that one row is one PatientICN-date
# ---------------------------------------------------------------------------
# dcast is data.table's equivalent of pivot_wider; fun.aggregate=max mirrors
# the previous behaviour of taking a single value per group.
pghd_final_wide <- dcast(
  pghd_final,
  PatientICN + date ~ device + measurement,
  value.var = "val",
  fun.aggregate = function(x) ifelse(length(x) == 0L, NA_real_, max(x, na.rm = TRUE)),
  sep = "_"
)

# Drop columns that are entirely NA
all_na_cols <- names(pghd_final_wide)[sapply(pghd_final_wide, function(x) all(is.na(x)))]
if (length(all_na_cols)) pghd_final_wide[, (all_na_cols) := NULL]

#pghd_final_wide <- pghd_final_wide[date < as.Date("2025-12-24")]

write_parquet(pghd_final_wide, 'data/input/pghd_final_wide.parquet')
pghd_final_wide <- open_dataset('data/input/pghd_final_wide.parquet')

# ---------------------------------------------------------------------------
# 4) Expand days; Need the analysis period to extend out to 7 days after the last day of data.
# ---------------------------------------------------------------------------
#extended_df = data.table(PatientICN = as.character(rep(999, 30)), date = seq(as.Date("2025-09-25"), as.Date("2026-01-22"), by = "day"))

# Collect the wide data and convert to data.table
pghd_wide_dt <- as.data.table(collect(pghd_final_wide))
pghd_wide_dt[, date := as.Date(date)]

# Get all unique PatientICN x date combinations observed in the data
unique_icn   <- pghd_wide_dt[, unique(PatientICN)]
unique_dates <- pghd_wide_dt[, unique(date)]

# CJ = cross-join (data.table's expand.grid); filter out dummy ICN
id_days <- CJ(PatientICN = unique_icn, date = unique_dates)[PatientICN != "999"]

# Left join expanded grid onto wide PGHD data
pghd_final_full <- pghd_wide_dt[id_days, on = .(PatientICN, date)]

setDT(pghd_final_full)

# Identify PGHD data days: any non-NA value across device-prefixed columns
device_cols <- grep("^(ah|fit|gar)_", names(pghd_final_full), value = TRUE)
pghd_final_full[,
                data_day := as.integer(rowSums(!is.na(.SD)) > 0L),
                .SDcols = device_cols
]

# Per-person date summaries (in-place, no copy)
pghd_final_full[,
                `:=`(
                  date_first = min(date[data_day == 1L], na.rm = TRUE),
                  date_last  = max(date[data_day == 1L], na.rm = TRUE)
                ),
                by = PatientICN
]
pghd_final_full[, obs_end := date_last %m+% months(1)]

# Reorder columns: identifiers first, then device prefixes
id_cols  <- c("PatientICN", "date", "data_day", "date_first", "date_last", "obs_end")
dev_cols <- grep("^(ah|fit|gar)_", names(pghd_final_full), value = TRUE)
setcolorder(pghd_final_full, c(id_cols, dev_cols))

# Meteorological season
pghd_final_full[, met_season := traumar::season(date)]

# Upload as parquet file:
write_parquet(pghd_final_full, 'data/input/pghd_final_full.parquet')

# ---------------------------------------------------------------------------
# 5) Finally, we create person-level summary data which has the first date of upload, and dates one year, two years, and five years before (adjusted for leap days) which we will use in subsequent code to extract prior demographic, vital, and health care utilization features.
# ---------------------------------------------------------------------------
pghd_final_full_visits_ids <- unique(
  pghd_final_full[, .(PatientICN, date_first, date_last, obs_end)]
)

feb29_2024 <- as.Date("2024-02-29")

pghd_final_full_visits_ids[, `:=`(
  one_years_prior_date  = fifelse(date_first == feb29_2024, as.Date("2023-02-28"), date_first %m-% years(1)),
  two_years_prior_date  = fifelse(date_first == feb29_2024, as.Date("2022-02-28"), date_first %m-% years(2)),
  five_years_prior_date = fifelse(date_first == feb29_2024, as.Date("2019-02-28"), date_first %m-% years(5)),
  last_plus_7           = fifelse(date_last  == feb29_2024, as.Date("2024-03-07"), date_last  %m+% days(7)),
  last_plus_30          = fifelse(date_last  == feb29_2024, as.Date("2024-03-30"), date_last  %m+% days(30))
)]

write_parquet(pghd_final_full_visits_ids,
              'data/pghd_final_full_visits_ids.parquet')
