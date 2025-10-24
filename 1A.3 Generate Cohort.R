### Nick Cardamone
### OCC_PGHDPred
### 1. Generate Cohort
### Date created: 4/29/2025
### Last updated: 10/23/2025
# ---------------------------------------------------------------------------
# Extract all individuals with any PGHD data from DOEx.GENERIC PGHD in the following categories, "Sleep", "Workout", and "Daily Activity Summary".
# Define the analysis period as 1/1/2022 to 9/25/2025 and extract all outcomes data to the end of the latest possible observation period as 10/24/2025.
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
  "C:\\Users\\VHAPHICardaN\\OneDrive - Department of Veterans Affairs\\Desktop\\Projects\\OPS_Bressman-PGHDPred\\"
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
# 1) Extract all individuals with any PGHD data from DOEx.GENERIC PGHD in the following categories, "Sleep", "Workout", and "Daily Activity Summary".
# ---------------------------------------------------------------------------
# If the test works, then run the full query
pghd_pghd <- tbl(db_doex_con, in_schema('DOEx', 'GENERIC_PGHD')) %>%
  transmute(
    PatientICN = ICN,
    date = as.Date(measurementDate),
    category,
    measurement,
    device,
    units,
    ObservationPID,
    value
  ) %>%
  filter(category %in% c("Sleep", "Workout", "Daily Activity Summary") &
           value >= 0) %>%
  group_by(PatientICN, date, category, measurement, device, units) %>%
  dplyr::summarize(
    n_obs = n(),
    val = max(value, na.rm = T) # arbitrary choice - if there are two values for a given measurement from a given device on the same day, take the max.
  )

# Save to parquet file:
pghd_pghd %>% write_parquet(
  'parquet/PGHD_PGHD.parquet'
)

# Load parquet:
pghd_pghd <- open_dataset(
  'parquet/PGHD_PGHD.parquet'
)

# ---------------------------------------------------------------------------
# 2) Now that we have cast a large net over the PGHD data we want to use, extract the SCRSSN, ensure that there are no bad identifiers, and create crosswalk tables.
#   Define the analysis period as 1/1/2022 to 9/25/2025 and extract all outcomes data to the end of the latest possible observation period as 10/24/2025.
#   Observation periods extend out a month after the last upload date for a given person.
#   Extract and clean the names of 48 PGHD features. Also, we are expanding the data set so that all days in the analysis period are listed for every person - this will help when we paste in the outcome variables (ED, inpatient, or death) later.
# ---------------------------------------------------------------------------

pghd_final <- pghd_pghd %>%
  filter(date >= "2022-01-01") %>%
  mutate(
    measurement = case_when(
      category == "Daily Activity Summary" &
        measurement == "Average Heart Rate for Daily Summary" ~ "das_avgHR_bpm",
      category == "Daily Activity Summary" &
        measurement == "Max Heart Rate Measured" ~ "das_maxHR_bpm",
      category == "Daily Activity Summary" &
        measurement == "Minimum Heart Rate Measured" ~ "das_minHR_bpm",
      measurement == "Heart Rate Variability" ~ "das_HRV_bpm",
      measurement == "Heart rate resting" ~ "das_HRResting_bpm",
      category == "Daily Activity Summary" &
        measurement == "Heart Rate Zone Very Low" ~ "das_HRVeyLow_sec",
      category == "Daily Activity Summary" &
        measurement == "Heart Rate Zone Low" ~ "das_HRLow_sec",
      category == "Daily Activity Summary" &
        measurement == "Heart Rate Zone Medium" ~ "das_HRMedium_sec",
      category == "Daily Activity Summary" &
        measurement == "Heart Rate Zone High" ~ "das_HRHigh_sec",
      measurement == "Basal metabolic rate index" ~ "das_BMRI_kcal",
      category == "Daily Activity Summary" &
        measurement == "Energy Burned (Calories)" ~ "das_burnedenergy_kcal",
      measurement == "Calories burned during activity" ~ "das_burnedactivity_kcal",
      measurement == "Elevation climbed [Length/Time] 24 hour" ~ "das_climbed_m",
      category == "Daily Activity Summary" &
        measurement == "Exercise duration" ~ "das_exercise_sec",
      category == "Daily Activity Summary" &
        measurement == "Exercise distance in 24 hour" ~ "das_exercise_m",
      category == "Daily Activity Summary" &
        measurement == "Number of steps in 24 hour Measured" ~ "das_exercise_steps",
      measurement == "Flights climbed 24 hour" ~ "das_exercise_flights",
      category == "Daily Activity Summary" &
        measurement == "Time Spent Fairly Active" ~ "das_FairlyActive_sec",
      category == "Daily Activity Summary" &
        measurement == "Time Spent Lightly Active" ~ "das_LightlyActive_sec",
      category == "Daily Activity Summary" &
        measurement == "Time Spent Meditating" ~ "das_Meditating_sec",
      category == "Daily Activity Summary" &
        measurement == "Time Spent Very Active" ~ "das_VeryActive_sec",
      measurement == "Average Cadence" ~ "wo_acadence_rmin",
      measurement == "Average Speed" ~ "wo_avgspeed_ms",
      measurement == "Maximum Speed Reached" ~ "wo_maxspeed_ms",
      measurement == "Average Heart Rate for Workout" ~ "wo_avgHR_bpm",
      category == "Workout" &
        measurement == "Max Heart Rate Measured" ~ "wo_maxHR_bpm",
      category == "Workout" &
        measurement == "Minimum Heart Rate Measured" ~ "wo_minHR_bpm",
      category == "Workout" &
        measurement == "Heart Rate Zone Very Low" ~ "wo_HRVeyLow_sec",
      category == "Workout" &
        measurement == "Heart Rate Zone Low" ~ "wo_HRLow_sec",
      category == "Workout" &
        measurement == "Heart Rate Zone Medium" ~ "wo_HRMedium_sec",
      category == "Workout" &
        measurement == "Heart Rate Zone High" ~ "wo_HRHigh_sec",
      category == "Workout" &
        measurement == "Energy Burned (Calories)" ~ "wo_burnedenergy_kcal",
      category == "Workout" &
        measurement == "Number of steps in 24 hour Measured" ~ "wo_exercise_steps",
      category == "Workout" &
        measurement == "Exercise distance in 24 hour" ~ "wo_exercise_m",
      category == "Workout" &
        measurement == "Exercise duration" ~ "wo_exercise_sec",
      category == "Workout" &
        measurement == "Time Spent Fairly Active" ~ "wo_FairlyActive_sec",
      category == "Workout" &
        measurement == "Time Spent Lightly Active" ~ "wo_LightlyActive_sec",
      category == "Workout" &
        measurement == "Time Spent Very Active" ~ "wo_VeryActive_sec",
      measurement == "Time in REM Sleep" ~ "sleep_REM_sec",
      measurement == "In Bed Duration" ~ "sleep_BED_sec",
      measurement == "Deep Sleep Duration" ~ "sleep_DEEP_sec",
      measurement == "Light Sleep Duration" ~ "sleep_LIGHT_sec",
      measurement == "Time Spent Awake" ~ "sleep_AWAKE_sec",
      measurement == "Total Sleep Duration" ~ "sleep_TOTAL_sec",
      measurement == "Times Awakened" ~ "sleep_awakened_count",
      measurement == "Sleep Score (Overall Quality)" ~ "sleep_quality_score",
      measurement == "Times Spent Restless" ~ "sleep_RESTLESS_sec",
      measurement == "Time Taken to Fall Asleep" ~ "sleep_FALLASLEEP_sec",
      TRUE ~ NA_character_
    ),
    device = case_when(
      device == "apple_health" ~ "ah",
      device == "fitbit" ~ "fit",
      device == "garmin" ~ "gar",
      device == "google_fit_sdk" ~ "goog"
    ),
    val = round(val, 2)
  ) %>%
  select(PatientICN, date, device, measurement, val)

# Because the patient-day-measure count for Google SDK is so much lower relative to Fitbit, Garmin, and Apple Health, we chose to drop all uploads from this device.

pghd_final <- pghd_final %>% filter(device != "goog")

# ---------------------------------------------------------------------------
# 3) Pivot so that one row is one PatientICN-date
# ---------------------------------------------------------------------------
pghd_final_wide <- pghd_final %>%
  collect() %>%
  pivot_wider(
    names_from = c("device", "measurement"),
    values_from = "val",
    names_sep = "_"
  ) %>%
  # drop empty columns
  select(where( ~ !all(is.na(.))))

pghd_final_wide %>% write_parquet('parquet\\pghd_final_wide.parquet')
pghd_final_wide <- open_dataset('parquet\\pghd_final_wide.parquet')

# ---------------------------------------------------------------------------
# 4) Expand days; Need the analysis period to extend out to 7 days after the last day of data.
# ---------------------------------------------------------------------------
extended_df = data.frame(PatientICN = as.character(rep(999, 30)),
                         date = seq(as.Date("2025-09-25"), as.Date("2025-10-24"), by = "day"))

# Get all PatientICN and dates in the data, expand to all dates in the data:
id_days = pghd_final_wide %>%
  ungroup() %>%
  transmute(PatientICN, date = as.Date(date)) %>%
  distinct() %>%
  collect() %>%
  rbind(extended_df)

id_days = id_days %>%
  expand(PatientICN, date) %>%
  filter(PatientICN != 999) # get rid of dummy ICN

# Expand data set so that all days in the analysis period (1-1-2022 to 10-24-2025 are listed for every person.)
pghd_final_full = left_join(
  id_days,
  pghd_final_wide %>% mutate(date = as.Date(date)) %>% collect(),
  by = c("PatientICN", "date")
)

pghd_final_full <- pghd_final_full %>%
  mutate(data_day = if_any(
    c(ah_das_exercise_sec:gar_wo_maxHR_bpm),
    ~ !is.na(.),
    .default = 0
  ),
  data_day = if_else(data_day, 1, 0)) %>%
  group_by(PatientICN) %>%
  mutate(
    date_first = min(date[data_day == 1], na.rm = T),
    date_last = max(date[data_day == 1], na.rm = T),
    obs_end = date_last %m+% months(1)
  ) %>%
  ungroup() %>%
  select(
    PatientICN,
    date,
    data_day,
    date_first,
    date_last,
    obs_end,
    starts_with("ah"),
    starts_with("fit"),
    starts_with("gar")
  ) %>%
  mutate(met_season = traumar::season(date)) # Meteorological season.

# Upload as parquet file:
write_parquet(pghd_final_full, 'parquet\\pghd_final_full.parquet')

# ---------------------------------------------------------------------------
# 5) Finally, we create person-level summary data which has the first date of upload, and dates one year, two years, and five years before (adjusted for leap days) which we will use in subsequent code to extract prior demographic, vital, and health care utilization features.
# ---------------------------------------------------------------------------
pghd_final_full_visits_ids <- pghd_final_full %>%
  select(PatientICN, date_first, date_last) %>% distinct() %>% collect() %>%
  mutate(
    one_years_prior_date = if_else(
      date_first == "2024-02-29",
      as.Date("2023-02-28", '%Y-%m-%d'),
      date_first %m-% years(1)
    ),
    two_years_prior_date = if_else(
      date_first == "2024-02-29",
      as.Date("2022-02-28", '%Y-%m-%d'),
      date_first %m-% years(2)
    ),
    five_years_prior_date = if_else(
      date_first == "2024-02-29",
      as.Date("2019-02-28", '%Y-%m-%d'),
      date_first %m-% years(5)
    ),
    last_plus_7 = if_else(
      date_first == "2024-02-29",
      as.Date("2019-02-28", '%Y-%m-%d'),
      date_last %m+% days(7)
    ),
    last_plus_30 = if_else(
      date_first == "2024-02-29",
      as.Date("2019-02-28", '%Y-%m-%d'),
      date_last %m+% days(30)
    )
  )

write_parquet(pghd_final_full_visits_ids,
              'parquet\\pghd_final_full_visits_ids.parquet')
