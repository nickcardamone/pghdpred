### Nick Cardamone
### OCC_PGHDPred
### 5. Vital signs and anthropometric measurements
### Date created: 4/29/2025
### Last updated: 5/15/2026

# Features of interest for modeling (all within timeframes specified below):

# 1. BMI (average prior 3 years) - continuous
# 2. BMI trajectory (prior 5 years) - spline
# 3. Diastolic Blood Pressure (most recent, prior year) - categorical  
# 4. Mean Arterial Pressure trajectory (prior 5 years) - spline
# 5. Heart Rate/Pulse (most recent, prior year) - categorical
# 6. Systolic Blood Pressure (most recent, prior year) - categorical

# All features derived from VitalSign table via OMOP Measurement concepts.
# MAP = DiastolicPressure + 1/3(SystolicPressure - DiastolicPressure)

suppressPackageStartupMessages({
library(DBI) # Working with data in databases
library(dbplyr) # Working with data in databases
library(dplyr)
library(data.table)
library(comorbidity) #processing elixhauser data
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
library(odbc)  # Added explicit odbc'
library(splines)
  
})

'%!in%' <- function(x,y)!('%in%'(x,y))

# Connect to DB using ODBC with RB03 profile
con <- dbConnect(odbc::odbc(), 
                 .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;", 
                 timeout = 10)

# Connect to specific databases using ODBC
cdwwork <- dbConnect(odbc::odbc(), 
                     .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;", 
                     timeout = 10,
                     database = "CDWWork")

db_pghpred <- dbConnect(odbc::odbc(), 
                        .connection_string = "Driver={SQL Server};Server=vhacdwrb03.vha.med.va.gov;Trusted_Connection=yes;", 
                        timeout = 10,
                        database = "OCC_PGHDPred")

# ---------------------------------------------------------------------------
# 1) Read cohort and map IDs (consistent with lab extraction pattern)
# ---------------------------------------------------------------------------

# Set working directory

setwd(
  "C://Users//VHAPHICardaN//OneDrive - Department of Veterans Affairs//Desktop//Projects//OPS_Bressman-PGHDPred//pghdpred_deliverable"
)

# Cohort: contains PatientICN and upload window variables (e.g. date_first)
cohort = open_dataset('data\\input\\pghd_final_full_visits_ids.parquet') %>% collect() %>% na.omit()

# ---------------------------------------------------------------------------
# M2 anchor: load index events for patients with qualifying ED/IP event
# ---------------------------------------------------------------------------
index_events_m2 <- open_dataset('data\\input\\index_event_prelim.parquet') %>%
  collect() %>%
  filter(!is.na(index_date)) %>%
  select(PatientICN, index_date) %>%
  mutate(index_date = as.Date(index_date))

cohort_m2 <- cohort %>%
  inner_join(index_events_m2, by = "PatientICN")

global_pull_end <- max(cohort_m2$index_date, na.rm = TRUE)
cat("M2 cohort:", nrow(cohort_m2), "patients; pull end:", as.character(global_pull_end), "\n")

# Map PatientICN -> PERSON_ID for OMOP joins
omop_xw <- tbl(cdwwork, in_schema('OMOPV5Map', 'SPatient_PERSON')) %>%
  inner_join(cohort, by = "PatientICN", copy = TRUE) %>%
  select(PatientICN, PERSON_ID, date_first, one_years_prior_date, two_years_prior_date, five_years_prior_date) %>%
  distinct()

# ---------------------------------------------------------------------------
# 2. Extract OMOP vital signs measurements for cohort (prior 5 years)
# ---------------------------------------------------------------------------

# Define OMOP concept IDs for vital signs based on standard mappings
vital_concepts <- list(
  weight       = c(3013762, 3003176, 3025315, 3023166, 3026600),  # Body weight concepts
  height       = c(3023540, 3019171, 3036277),                    # Body height concepts
  systolic_bp  = c(3004249, 3018586, 3028737),                    # Systolic blood pressure
  diastolic_bp = c(3012888, 3034703, 3019962),                    # Diastolic blood pressure
  heart_rate   = c(3027018, 3027598, 3018567),                    # Heart rate/pulse
  spo2         = c(40762499L, 3016335L),                          # Oxygen saturation by pulse oximetry
  temperature  = c(3020891L, 36031613L)                           # Body temperature (oral, axillary)
)

# Get OMOP concept metadata
omop_concept <- tbl(cdwwork, in_schema('OMOPV5', 'CONCEPT')) %>% 
  filter(CONCEPT_ID %in% !!unlist(vital_concepts)) %>%
  select(CONCEPT_ID, CONCEPT_NAME, DOMAIN_ID) %>%
  collect()

# Extract vital signs measurements — broad scalar pull; per-patient anchor applied in R
# days_before_index >= 0 inside processing functions filters to M1 (date_first) automatically.
omop_vitals <- tbl(cdwwork, in_schema('OMOPV5', 'MEASUREMENT')) %>%
  inner_join(omop_xw, by = "PERSON_ID") %>%
  filter(MEASUREMENT_CONCEPT_ID %in% !!unlist(vital_concepts)) %>%
  filter(MEASUREMENT_DATE >= '2018-01-01' & MEASUREMENT_DATE <= !!as.character(global_pull_end)) %>%
  select(PatientICN, PERSON_ID, MEASUREMENT_CONCEPT_ID, MEASUREMENT_DATE,
         VALUE_AS_NUMBER, UNIT_CONCEPT_ID, UNIT_SOURCE_VALUE, date_first) %>%
  collect()

# Join with concept names for interpretation
omop_vitals <- omop_vitals %>%
  left_join(omop_concept, by = c("MEASUREMENT_CONCEPT_ID" = "CONCEPT_ID"))

# Save raw vitals data
write_parquet(omop_vitals, 'data\\omop_vitals_raw.parquet')

# M2 vitals: replace date_first with index_date so all processing functions use index_date as anchor
omop_vitals_m2 <- omop_vitals %>%
  inner_join(cohort_m2 %>% select(PatientICN, index_date), by = "PatientICN") %>%
  mutate(date_first = index_date)  # override anchor; days_before_index computed off this

cat("M2 vitals rows:", nrow(omop_vitals_m2), "\n")
# ---------------------------------------------------------------------------
# 3) Process weight measurements and calculate BMI
# ---------------------------------------------------------------------------

# Process weight measurements with unit normalization
process_weight <- function(vitals_data) {
  weight_data <- vitals_data %>%
    filter(MEASUREMENT_CONCEPT_ID %in% vital_concepts$weight) %>%
    filter(!is.na(VALUE_AS_NUMBER), VALUE_AS_NUMBER > 0) %>%
    mutate(
      # Normalize weight to kg based on unit concept ID
      weight_kg = case_when(
        UNIT_CONCEPT_ID %in% c(0, 4124425) ~ VALUE_AS_NUMBER * 0.453592,  # lbs to kg
        UNIT_CONCEPT_ID == 4122383 ~ VALUE_AS_NUMBER,  # already kg
        UNIT_CONCEPT_ID %in% c(8504, 9502) ~ VALUE_AS_NUMBER / 1000,  # g to kg
        UNIT_CONCEPT_ID == 8576 ~ VALUE_AS_NUMBER * 0.453592,  # mg listed but likely lbs
        TRUE ~ VALUE_AS_NUMBER * 0.453592  # default assume lbs
      )
    ) %>%
    # Filter reasonable weight ranges (in kg)
    filter(weight_kg >= 20 & weight_kg <= 300) %>%
    select(PatientICN, MEASUREMENT_DATE, weight_kg, date_first)
  
  return(weight_data)
}

# Process height measurements with unit normalization  
process_height <- function(vitals_data) {
  height_data <- vitals_data %>%
    filter(MEASUREMENT_CONCEPT_ID %in% vital_concepts$height) %>%
    filter(!is.na(VALUE_AS_NUMBER), VALUE_AS_NUMBER > 0) %>%
    mutate(
      # Normalize height to cm based on unit concept ID and value ranges
      height_cm = case_when(
        # Height in cm (value between 100-250)
        MEASUREMENT_CONCEPT_ID == 3036277 & UNIT_CONCEPT_ID == 0 & VALUE_AS_NUMBER >= 100 & VALUE_AS_NUMBER <= 250 ~ VALUE_AS_NUMBER,
        # Height in inches (value between 40-100)  
        MEASUREMENT_CONCEPT_ID == 3036277 & UNIT_CONCEPT_ID == 0 & VALUE_AS_NUMBER >= 40 & VALUE_AS_NUMBER < 100 ~ VALUE_AS_NUMBER * 2.54,
        # Height in feet (value between 3-8)
        MEASUREMENT_CONCEPT_ID == 3036277 & UNIT_CONCEPT_ID == 0 & VALUE_AS_NUMBER >= 3 & VALUE_AS_NUMBER <= 8 ~ VALUE_AS_NUMBER * 30.48,
        # Explicit feet unit
        UNIT_CONCEPT_ID == 4118332 ~ VALUE_AS_NUMBER * 30.48,
        # Other cases - assume inches if reasonable range
        MEASUREMENT_CONCEPT_ID == 3019171 & VALUE_AS_NUMBER >= 40 & VALUE_AS_NUMBER < 100 ~ VALUE_AS_NUMBER * 2.54,
        TRUE ~ VALUE_AS_NUMBER * 2.54  # default assume inches
      )
    ) %>%
    # Filter reasonable height ranges (in cm)
    filter(height_cm >= 120 & height_cm <= 220) %>%
    select(PatientICN, MEASUREMENT_DATE, height_cm)
  
  return(height_data)
}

# Calculate BMI for each patient
calculate_bmi <- function(weight_data, height_data) {
  # Get average height per patient (height is relatively stable)
  avg_height <- height_data %>%
    group_by(PatientICN) %>%
    summarise(avg_height_cm = mean(height_cm, na.rm = TRUE), .groups = 'drop')
  
  # Calculate BMI for each weight measurement
  bmi_data <- weight_data %>%
    left_join(avg_height, by = "PatientICN") %>%
    filter(!is.na(avg_height_cm)) %>%
    mutate(
      bmi = round((weight_kg / (avg_height_cm / 100)^2), 1),
      days_before_index = as.numeric(ymd(date_first) - ymd(MEASUREMENT_DATE))
    ) %>%
    # Filter reasonable BMI ranges
    filter(bmi >= 10 & bmi <= 80) %>%
    filter(days_before_index >= 0 & days_before_index <= 1825) %>%  # Within 5 years prior
    select(PatientICN, MEASUREMENT_DATE, weight_kg, avg_height_cm, bmi, days_before_index, date_first)
  
  return(bmi_data)
}

# Process weight and height data
weight_processed <- process_weight(omop_vitals)
height_processed <- process_height(omop_vitals)
bmi_calculated <- calculate_bmi(weight_processed, height_processed)

# Save BMI data
write_parquet(bmi_calculated, 'data\\bmi_calculated.parquet')

# ---------------------------------------------------------------------------
# 4) Process blood pressure and calculate MAP
# ---------------------------------------------------------------------------

# Process systolic blood pressure
process_systolic_bp <- function(vitals_data) {
  sbp_data <- vitals_data %>%
    filter(MEASUREMENT_CONCEPT_ID %in% vital_concepts$systolic_bp) %>%
    filter(!is.na(VALUE_AS_NUMBER)) %>%
    filter(VALUE_AS_NUMBER >= 60 & VALUE_AS_NUMBER <= 300) %>%  # Reasonable SBP range
    mutate(
      days_before_index = as.numeric(ymd(date_first) - ymd(MEASUREMENT_DATE)),
      sbp = VALUE_AS_NUMBER
    ) %>%
    filter(days_before_index >= 0 & days_before_index <= 1825) %>%  # Within 5 years prior
    select(PatientICN, MEASUREMENT_DATE, sbp, days_before_index, date_first)
  
  return(sbp_data)
}

# Process diastolic blood pressure
process_diastolic_bp <- function(vitals_data) {
  dbp_data <- vitals_data %>%
    filter(MEASUREMENT_CONCEPT_ID %in% vital_concepts$diastolic_bp) %>%
    filter(!is.na(VALUE_AS_NUMBER)) %>%
    filter(VALUE_AS_NUMBER >= 30 & VALUE_AS_NUMBER <= 150) %>%  # Reasonable DBP range
    mutate(
      days_before_index = as.numeric(ymd(date_first) - ymd(MEASUREMENT_DATE)),
      dbp = VALUE_AS_NUMBER
    ) %>%
    filter(days_before_index >= 0 & days_before_index <= 1825) %>%  # Within 5 years prior
    select(PatientICN, MEASUREMENT_DATE, dbp, days_before_index, date_first)
  
  return(dbp_data)
}

# Calculate Mean Arterial Pressure (MAP)
calculate_map <- function(sbp_data, dbp_data) {
  # Join SBP and DBP by patient and date (same measurement session)
  bp_combined <- sbp_data %>%
    full_join(dbp_data, by = c("PatientICN", "MEASUREMENT_DATE", "days_before_index", "date_first")) %>%
    filter(!is.na(sbp) & !is.na(dbp)) %>%
    mutate(
      # MAP = DBP + 1/3(SBP - DBP)
      map = dbp + (1/3) * (sbp - dbp)
    ) %>%
    # Filter reasonable MAP range
    filter(map >= 40 & map <= 200) %>%
    select(PatientICN, MEASUREMENT_DATE, sbp, dbp, map, days_before_index, date_first)
  
  return(bp_combined)
}

# Process blood pressure data
sbp_processed <- process_systolic_bp(omop_vitals)
dbp_processed <- process_diastolic_bp(omop_vitals)
bp_map_calculated <- calculate_map(sbp_processed, dbp_processed)

# Save blood pressure and MAP data
write_parquet(bp_map_calculated, 'data\\bp_map_calculated.parquet')

# ---------------------------------------------------------------------------
# 4b) Process SpO2 (pulse oximetry) measurements
# ---------------------------------------------------------------------------

process_spo2 <- function(vitals_data) {
  vitals_data %>%
    filter(MEASUREMENT_CONCEPT_ID %in% vital_concepts$spo2) %>%
    filter(!is.na(VALUE_AS_NUMBER), VALUE_AS_NUMBER >= 70, VALUE_AS_NUMBER <= 100) %>%
    mutate(
      days_before_index = as.numeric(ymd(date_first) - ymd(MEASUREMENT_DATE)),
      spo2 = VALUE_AS_NUMBER
    ) %>%
    filter(days_before_index >= 0 & days_before_index <= 365) %>%
    select(PatientICN, MEASUREMENT_DATE, spo2, days_before_index, date_first)
}

# ---------------------------------------------------------------------------
# 4c) Process temperature measurements (normalize to Celsius)
# ---------------------------------------------------------------------------

process_temperature <- function(vitals_data) {
  vitals_data %>%
    filter(MEASUREMENT_CONCEPT_ID %in% vital_concepts$temperature) %>%
    filter(!is.na(VALUE_AS_NUMBER), VALUE_AS_NUMBER > 0) %>%
    mutate(
      # If value >= 85 it is clearly Fahrenheit; otherwise assume Celsius
      temp_c = if_else(VALUE_AS_NUMBER >= 85,
                       (VALUE_AS_NUMBER - 32) / 1.8,
                       VALUE_AS_NUMBER)
    ) %>%
    filter(temp_c >= 35 & temp_c <= 42) %>%
    mutate(
      days_before_index = as.numeric(ymd(date_first) - ymd(MEASUREMENT_DATE)),
      temperature = temp_c
    ) %>%
    filter(days_before_index >= 0 & days_before_index <= 365) %>%
    select(PatientICN, MEASUREMENT_DATE, temperature, days_before_index, date_first)
}

spo2_processed        <- process_spo2(omop_vitals)
temperature_processed <- process_temperature(omop_vitals)

# ---------------------------------------------------------------------------
# 5) Create 5-year trajectory datasets for spline modeling
# ---------------------------------------------------------------------------

# Function to prepare 5-year trajectory data for splines
# Averages multiple measurements per day before creating trajectory
get_prior_year_trajectory <- function(data, value_col, patient_col = "PatientICN") {
  data %>%
    filter(days_before_index >= 0 & days_before_index <= 365) %>%  # Within 1 years prior
    # Average multiple measurements on the same day
    group_by(!!sym(patient_col), MEASUREMENT_DATE, days_before_index) %>%
    summarise(!!sym(value_col) := mean(!!sym(value_col), na.rm = TRUE), .groups = 'drop') %>%
    # Now group by patient and filter for those with 3+ measurement days
    group_by(!!sym(patient_col)) %>%
    arrange(days_before_index) %>%
    mutate(
      measurement_number = row_number(),
      total_measurements = n()
    ) %>%
    filter(total_measurements >= 1) %>%  # Keep all patients; sparse ones fall back to raw stats
    ungroup() %>%
    select(!!sym(patient_col), days_before_index, MEASUREMENT_DATE, !!sym(value_col), 
           measurement_number, total_measurements)
}

# Function to prepare 5-year trajectory data for splines
# Averages multiple measurements per day before creating trajectory
get_five_year_trajectory <- function(data, value_col, patient_col = "PatientICN") {
  data %>%
    filter(days_before_index >= 0 & days_before_index <= 1825) %>%  # Within 5 years prior
    # Average multiple measurements on the same day
    group_by(!!sym(patient_col), MEASUREMENT_DATE, days_before_index) %>%
    summarise(!!sym(value_col) := mean(!!sym(value_col), na.rm = TRUE), .groups = 'drop') %>%
    # Now group by patient and filter for those with 1+ measurement days
    group_by(!!sym(patient_col)) %>%
    arrange(days_before_index) %>%
    mutate(
      measurement_number = row_number(),
      total_measurements = n()
    ) %>%
    filter(total_measurements >= 1) %>%  # Keep all patients; sparse ones fall back to raw stats
    ungroup() %>%
    select(!!sym(patient_col), days_before_index, MEASUREMENT_DATE, !!sym(value_col), 
           measurement_number, total_measurements)
}

# Create BMI 5-year trajectory for spline modeling
bmi_trajectory_5yr <- get_five_year_trajectory(bmi_calculated, "bmi")

# Create MAP 5-year trajectory for spline modeling
map_trajectory_5yr <- get_five_year_trajectory(bp_map_calculated, "map")

# Save trajectory datasets
write_parquet(bmi_trajectory_5yr, 'data\\bmi_trajectory_5yr.parquet')
write_parquet(map_trajectory_5yr, 'data\\map_trajectory_5yr.parquet')

# ---------------------------------------------------------------------------
# 6) Summary statistics for trajectories
# ---------------------------------------------------------------------------

cat("=== BMI Trajectory Summary (5-year) ===\n")
bmi_traj_summary <- bmi_trajectory_5yr %>%
  group_by(PatientICN) %>%
  summarise(
    n_measurements = n(),
    timespan_days = max(days_before_index) - min(days_before_index),
    bmi_min = min(bmi),
    bmi_max = max(bmi),
    bmi_mean = mean(bmi),
    .groups = 'drop'
  )

cat("Patients with BMI trajectory:", nrow(bmi_traj_summary), "\n")
cat("Average measurements per patient:", round(mean(bmi_traj_summary$n_measurements), 1), "\n")
cat("Average timespan (days):", round(mean(bmi_traj_summary$timespan_days), 0), "\n\n")

cat("=== MAP Trajectory Summary (5-year) ===\n")
map_traj_summary <- map_trajectory_5yr %>%
  group_by(PatientICN) %>%
  summarise(
    n_measurements = n(),
    timespan_days = max(days_before_index) - min(days_before_index),
    map_min = min(map),
    map_max = max(map),
    map_mean = mean(map),
    .groups = 'drop'
  )

cat("Patients with MAP trajectory:", nrow(map_traj_summary), "\n")
cat("Average measurements per patient:", round(mean(map_traj_summary$n_measurements), 1), "\n")
cat("Average timespan (days):", round(mean(map_traj_summary$timespan_days), 0), "\n\n")

# ---------------------------------------------------------------------------
# 7) Fit splines and extract summary statistics per person
# ---------------------------------------------------------------------------

# Function to fit spline and extract summary stats for each patient
fit_spline_summary <- function(trajectory_data, value_col) {
  ns <- splines::ns  # bind locally so formula environment can resolve ns()

  # --- (A) Non-spline statistics ---
  base_stats <- trajectory_data %>%
    dplyr::group_by(PatientICN) %>%
    dplyr::summarise(
      n_measurements = dplyr::n(),
      timespan_days  = max(days_before_index) - min(days_before_index),
      value_min      = min(!!sym(value_col)),
      value_max      = max(!!sym(value_col)),
      value_mean     = mean(!!sym(value_col)),
      value_sd       = sd(!!sym(value_col)),
      value_range    = max(!!sym(value_col)) - min(!!sym(value_col)),
      value_first    = {
        d <- dplyr::pick(dplyr::everything())
        d[[value_col]][which.max(d$days_before_index)]
      },
      value_last     = {
        d <- dplyr::pick(dplyr::everything())
        d[[value_col]][which.min(d$days_before_index)]
      },
      slope          = {
        d  <- dplyr::pick(dplyr::everything())
        ts <- max(d$days_before_index) - min(d$days_before_index)
        if (ts > 0)
          (d[[value_col]][which.min(d$days_before_index)] -
           d[[value_col]][which.max(d$days_before_index)]) / ts * 365
        else NA_real_
      },
      .groups = "drop"
    )

  # --- (B) Spline statistics via group_modify (data passed explicitly) ---
  # Avoids cur_data() / list({}) interaction that silently returns empty frames
  # in dplyr >= 1.1.0 and causes all fitted_* to be NA.
  spline_stats <- trajectory_data %>%
    dplyr::group_by(PatientICN) %>%
    dplyr::group_modify(function(d, key) {
      n_uniq <- length(unique(d$days_before_index))
      na_row <- tibble::tibble(
        fitted_min = NA_real_, fitted_max  = NA_real_, fitted_mean  = NA_real_,
        fitted_sd  = NA_real_, fitted_range = NA_real_,
        fitted_first = NA_real_, fitted_last = NA_real_
      )
      if (nrow(d) < 4L || n_uniq < 2L) return(na_row)
      df_ns <- min(3L, n_uniq - 1L)
      tryCatch({
        f   <- as.formula(
          paste0(value_col, " ~ ns(days_before_index, df = ", df_ns, ")"),
          env = environment()          # environment() has ns in its parent chain
        )
        mod <- lm(f, data = d)
        fv  <- fitted(mod)
        tibble::tibble(
          fitted_min   = min(fv),
          fitted_max   = max(fv),
          fitted_mean  = mean(fv),
          fitted_sd    = sd(fv),
          fitted_range = max(fv) - min(fv),
          fitted_first = fv[which.max(d$days_before_index)],
          fitted_last  = fv[which.min(d$days_before_index)]
        )
      }, error = function(e) na_row)
    }) %>%
    dplyr::ungroup()

  dplyr::left_join(base_stats, spline_stats, by = "PatientICN") %>%
    # For patients with < 3 measurements, spline fitted_* are NA; fall back to raw equivalents
    dplyr::mutate(
      fitted_mean = dplyr::coalesce(fitted_mean, value_mean),
      fitted_last = dplyr::coalesce(fitted_last, value_last)
    )
}
# Fit splines and extract summary statistics for BMI
bmi_spline_summary <- fit_spline_summary(bmi_trajectory_5yr, "bmi")

# Rename columns for clarity
bmi_spline_summary <- bmi_spline_summary %>%
  rename_with(~paste0("bmi_", .), -PatientICN)

# Fit splines and extract summary statistics for MAP
map_spline_summary <- fit_spline_summary(map_trajectory_5yr, "map")

# Rename columns for clarity
map_spline_summary <- map_spline_summary %>%
  rename_with(~paste0("map_", .), -PatientICN)

cat("MAP spline summaries calculated for", nrow(map_spline_summary), "patients\n")

# Save spline summary statistics
write_parquet(bmi_spline_summary, 'data\\bmi_spline_summary.parquet')
write_parquet(map_spline_summary, 'data\\map_spline_summary.parquet')

# ---------------------------------------------------------------------------
# 7b) SBP and DBP 5-year spline summaries
# ---------------------------------------------------------------------------

sbp_trajectory_5yr <- get_five_year_trajectory(sbp_processed, "sbp")
dbp_trajectory_5yr <- get_five_year_trajectory(dbp_processed, "dbp")

sbp_spline_summary <- fit_spline_summary(sbp_trajectory_5yr, "sbp") %>%
  rename_with(~paste0("sbp_", .), -PatientICN)

dbp_spline_summary <- fit_spline_summary(dbp_trajectory_5yr, "dbp") %>%
  rename_with(~paste0("dbp_", .), -PatientICN)

cat("SBP spline summaries:", nrow(sbp_spline_summary), "patients\n")
cat("DBP spline summaries:", nrow(dbp_spline_summary), "patients\n")

write_parquet(sbp_trajectory_5yr, 'data\\sbp_trajectory_5yr.parquet')
write_parquet(dbp_trajectory_5yr, 'data\\dbp_trajectory_5yr.parquet')
write_parquet(sbp_spline_summary, 'data\\sbp_spline_summary.parquet')
write_parquet(dbp_spline_summary, 'data\\dbp_spline_summary.parquet')

# ---------------------------------------------------------------------------
# 7c) SpO2 and Temperature 1-year spline summaries
# ---------------------------------------------------------------------------

spo2_trajectory_1yr <- get_prior_year_trajectory(spo2_processed, "spo2")
temp_trajectory_1yr <- get_prior_year_trajectory(temperature_processed, "temperature")

spo2_spline_summary <- fit_spline_summary(spo2_trajectory_1yr, "spo2") %>%
  rename_with(~paste0("spo2_", .), -PatientICN)

temp_spline_summary <- fit_spline_summary(temp_trajectory_1yr, "temperature") %>%
  rename_with(~paste0("temp_", .), -PatientICN)

cat("SpO2 spline summaries:", nrow(spo2_spline_summary), "patients\n")
cat("Temp spline summaries:", nrow(temp_spline_summary), "patients\n")

write_parquet(spo2_trajectory_1yr, 'data\\spo2_trajectory_1yr.parquet')
write_parquet(temp_trajectory_1yr, 'data\\temp_trajectory_1yr.parquet')
write_parquet(spo2_spline_summary, 'data\\spo2_spline_summary.parquet')
write_parquet(temp_spline_summary, 'data\\temp_spline_summary.parquet')

# ---------------------------------------------------------------------------
# 8) Extract most recent pulse (heart rate) within prior year
# ---------------------------------------------------------------------------

# Process heart rate measurements
hr_processed <- omop_vitals %>%
  filter(MEASUREMENT_CONCEPT_ID %in% vital_concepts$heart_rate) %>%
  filter(!is.na(VALUE_AS_NUMBER)) %>%
  filter(VALUE_AS_NUMBER >= 30 & VALUE_AS_NUMBER <= 300) %>%  # Reasonable HR range
  mutate(
    days_before_index = as.numeric(ymd(date_first) - ymd(MEASUREMENT_DATE)),
    heart_rate = VALUE_AS_NUMBER
  ) %>%
  filter(days_before_index >= 0 & days_before_index <= 365) %>%  # Within prior year
  select(PatientICN, MEASUREMENT_DATE, heart_rate, days_before_index, date_first)

# Get most recent pulse for each patient
most_recent_pulse <- hr_processed %>%
  group_by(PatientICN) %>%
  arrange(days_before_index) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  select(PatientICN, recent_pulse = heart_rate, pulse_days_before = days_before_index)

# Save most recent pulse
write_parquet(most_recent_pulse, 'data\\most_recent_pulse.parquet')

# ---------------------------------------------------------------------------
# 9. Extract and process lab measurements (prior year splines)
# ---------------------------------------------------------------------------

# Define LOINC codes for lab measurements
lab_loinc_codes <- list(
  bun = c('3094-0', '6299-2', '11064-3'),
  albumin = c('1751-7', '2862-1', '1747-5', '1754-1', '6942-7', '1755-8', 
              '21059-1', '6941-9', '29946-1', '1753-3', '51190-7'),
  leukocytes = c('6690-2', '26464-8', '804-5')
)

# Get OMOP concept IDs from LOINC codes
loinc_to_concept <- tbl(cdwwork, in_schema('OMOPV5', 'CONCEPT')) %>%
  filter(VOCABULARY_ID == 'LOINC') %>%
  filter(CONCEPT_CODE %in% !!unlist(lab_loinc_codes)) %>%
  select(CONCEPT_ID, CONCEPT_CODE, CONCEPT_NAME) %>%
  collect()

# Create mapping of lab type to concept IDs
lab_concepts <- list(
  bun = loinc_to_concept %>% filter(CONCEPT_CODE %in% lab_loinc_codes$bun) %>% pull(CONCEPT_ID),
  albumin = loinc_to_concept %>% filter(CONCEPT_CODE %in% lab_loinc_codes$albumin) %>% pull(CONCEPT_ID),
  leukocytes = loinc_to_concept %>% filter(CONCEPT_CODE %in% lab_loinc_codes$leukocytes) %>% pull(CONCEPT_ID)
)

# Extract lab measurements — broad scalar pull; days_before_index filter applied inside processing
omop_labs <- tbl(cdwwork, in_schema('OMOPV5', 'MEASUREMENT')) %>%
  inner_join(omop_xw, by = "PERSON_ID") %>%
  filter(MEASUREMENT_CONCEPT_ID %in% !!unlist(lab_concepts)) %>%
  filter(MEASUREMENT_DATE >= '2022-01-01' & MEASUREMENT_DATE <= !!as.character(global_pull_end)) %>%
  select(PatientICN, PERSON_ID, MEASUREMENT_CONCEPT_ID, MEASUREMENT_DATE,
         VALUE_AS_NUMBER, UNIT_CONCEPT_ID, UNIT_SOURCE_VALUE, date_first) %>%
  collect()

# M2 labs: replace anchor date
omop_labs_m2 <- omop_labs %>%
  inner_join(cohort_m2 %>% select(PatientICN, index_date), by = "PatientICN") %>%
  mutate(date_first = index_date)

# Process BUN measurements
# Most common unit: mg/dL
bun_data <- omop_labs %>%
  filter(MEASUREMENT_CONCEPT_ID %in% lab_concepts$bun) %>%
  filter(!is.na(VALUE_AS_NUMBER), VALUE_AS_NUMBER > 0) %>%
  mutate(
    # Standardize all BUN values to mg/dL
    bun_mgdl = case_when(
      tolower(UNIT_SOURCE_VALUE) %in% c('mg/dl', 'mg/dl', 'mg/dl') ~ VALUE_AS_NUMBER,
      tolower(UNIT_SOURCE_VALUE) == 'mg/ml' ~ VALUE_AS_NUMBER * 1000,  # mg/mL to mg/dL (unlikely but handle)
      TRUE ~ VALUE_AS_NUMBER  # Default assume mg/dL
    )
  ) %>%
  # Apply reasonable range filter AFTER unit standardization
  filter(bun_mgdl >= 1 & bun_mgdl <= 200) %>%  # Reasonable BUN range (mg/dL)
  mutate(
    days_before_index = as.numeric(ymd(date_first) - ymd(MEASUREMENT_DATE)),
    bun = bun_mgdl
  ) %>%
  filter(days_before_index >= 0 & days_before_index <= 365) %>%
  select(PatientICN, MEASUREMENT_DATE, bun, days_before_index, date_first)

# Process Albumin measurements with unit standardization
albumin_data <- omop_labs %>%
  filter(MEASUREMENT_CONCEPT_ID %in% lab_concepts$albumin) %>%
  filter(!is.na(VALUE_AS_NUMBER), VALUE_AS_NUMBER > 0) %>%
  mutate(
    # Standardize all Albumin values to g/dL
    albumin_gdl = case_when(
      tolower(UNIT_SOURCE_VALUE) %in% c('g/dl', 'g/dl', 'g/dl', 'gm/dl', 'gm/dl', 'gm/dl') ~ VALUE_AS_NUMBER,
      tolower(UNIT_SOURCE_VALUE) %in% c('mg/dl', 'mg/dl') ~ VALUE_AS_NUMBER / 1000,  # mg/dL to g/dL
      tolower(UNIT_SOURCE_VALUE) %in% c('mg/l', 'mg/l') ~ VALUE_AS_NUMBER / 10000,  # mg/L to g/dL
      tolower(UNIT_SOURCE_VALUE) == 'ug/ml' ~ VALUE_AS_NUMBER / 1000,  # ug/mL to g/dL
      tolower(UNIT_SOURCE_VALUE) == '%' ~ NA,  # Assume % of 100 reference
      TRUE ~ VALUE_AS_NUMBER  # Default assume g/dL
    )
  ) %>%
  # Apply reasonable range filter AFTER unit standardization
  filter(albumin_gdl >= 1 & albumin_gdl <= 7) %>%  # Reasonable Albumin range (g/dL)
  mutate(
    days_before_index = as.numeric(ymd(date_first) - ymd(MEASUREMENT_DATE)),
    albumin = albumin_gdl
  ) %>%
  filter(days_before_index >= 0 & days_before_index <= 365) %>%
  select(PatientICN, MEASUREMENT_DATE, albumin, days_before_index, date_first)

# Process Leukocytes measurements with unit standardization
# Standard target: K/uL (thousands per microliter)
leukocytes_data <- omop_labs %>%
  filter(MEASUREMENT_CONCEPT_ID %in% lab_concepts$leukocytes) %>%
  filter(!is.na(VALUE_AS_NUMBER), VALUE_AS_NUMBER > 0) %>%
  mutate(
    # Standardize all Leukocyte values to K/uL (thousands per microliter)
    leukocytes_kul = case_when(
      # Already in K/uL or equivalent units (K/cmm, K/cumm, K/mm3, K/mcL all equivalent)
      tolower(UNIT_SOURCE_VALUE) %in% c('k/ul', 'k/ul', 'k/cmm', 'k/cumm', 'k/mm3', 'k/mm3', 
                                        'k/mcl', 'k/µl', 'thou/ul', 'thou/ul', 'thousand/ul',
                                        'thou.cmm', 'thou/cumm') ~ VALUE_AS_NUMBER,
      # Already expressed as thousands
      tolower(UNIT_SOURCE_VALUE) %in% c('10*3/ul', '10*3/ul', '10**3/ul', '10e3/ul', '10e3/ul',
                                        '10e3/mcl', '10.e3/ul', '10~u~3/ul', '10e9/l',
                                        'x10*3/ul', 'x1000/ul', 'x10e3/ul', 'x10e3/ul', 'x10e3/ul',
                                        '10x3/ul', '10x3/cmm', '10x3cumm', 't/cmm',
                                        'x10-3/ul', 'x10-3/ul', 'x10(3)/ul', 'x10 3') ~ VALUE_AS_NUMBER,
      # 10^3/uL variants
      tolower(UNIT_SOURCE_VALUE) %in% c('10 3/ ul', '10(3)/mcl') ~ VALUE_AS_NUMBER,
      # Per uL (need to divide by 1000 to get K/uL)
      tolower(UNIT_SOURCE_VALUE) %in% c('/ul', '1000/ul', 'cells/ul', 'cells /mcl') ~ VALUE_AS_NUMBER / 1000,
      # BILL/L (billions per liter = 10^9/L = K/uL)
      tolower(UNIT_SOURCE_VALUE) == 'bill/l' ~ VALUE_AS_NUMBER,
      # k/cmm variations (already thousands)
      tolower(UNIT_SOURCE_VALUE) %in% c('k/cmm', 'k/ul') ~ VALUE_AS_NUMBER,
      TRUE ~ VALUE_AS_NUMBER  # Default assume K/uL
    )
  ) %>%
  # Apply reasonable range filter AFTER unit standardization
  filter(leukocytes_kul >= 0.5 & leukocytes_kul <= 100) %>%  # Reasonable WBC range (K/uL)
  mutate(
    days_before_index = as.numeric(ymd(date_first) - ymd(MEASUREMENT_DATE)),
    leukocytes = leukocytes_kul
  ) %>%
  filter(days_before_index >= 0 & days_before_index <= 365) %>%
  select(PatientICN, MEASUREMENT_DATE, leukocytes, days_before_index, date_first)

cat("Leukocytes measurements:", nrow(leukocytes_data), "\n\n")

# Create trajectories
bun_trajectory_1yr <- get_prior_year_trajectory(bun_data, "bun")
albumin_trajectory_1yr <- get_prior_year_trajectory(albumin_data, "albumin")
leukocytes_trajectory_1yr <- get_prior_year_trajectory(leukocytes_data, "leukocytes")

# Fit splines and extract summary statistics for BMI
bun_spline_summary <- fit_spline_summary(bun_trajectory_1yr, "bun")

# Rename columns for clarity
bun_spline_summary <- bun_spline_summary %>%
  rename_with(~paste0("bun_", .), -PatientICN)

# Fit splines and extract summary statistics for MAP
albumin_spline_summary <- fit_spline_summary(albumin_trajectory_1yr, "albumin")

# Rename columns for clarity
albumin_spline_summary <- albumin_spline_summary %>%
  rename_with(~paste0("albumin_", .), -PatientICN)

# Fit splines and extract summary statistics for MAP
leukocytes_spline_summary <- fit_spline_summary(leukocytes_trajectory_1yr, "leukocytes")

# Rename columns for clarity
leukocytes_spline_summary <- leukocytes_spline_summary %>%
  rename_with(~paste0("leuk_", .), -PatientICN)


# Save lab trajectory and spline data
write_parquet(bun_trajectory_1yr, 'data\\bun_trajectory_1yr.parquet')
write_parquet(albumin_trajectory_1yr, 'data\\albumin_trajectory_1yr.parquet')
write_parquet(leukocytes_trajectory_1yr, 'data\\leukocytes_trajectory_1yr.parquet')

write_parquet(bun_spline_summary, 'data\\bun_spline_summary.parquet')
write_parquet(albumin_spline_summary, 'data\\albumin_spline_summary.parquet')
write_parquet(leukocytes_spline_summary, 'data\\leukocytes_spline_summary.parquet')

# ---------------------------------------------------------------------------
# 10) Create final combined feature dataset
# ---------------------------------------------------------------------------

# Combine all features
final_vitals_features <- cohort %>%
  select(PatientICN, date_first) %>%
  left_join(bmi_spline_summary,        by = "PatientICN") %>%
  left_join(map_spline_summary,        by = "PatientICN") %>%
  left_join(sbp_spline_summary,        by = "PatientICN") %>%
  left_join(dbp_spline_summary,        by = "PatientICN") %>%
  left_join(spo2_spline_summary,       by = "PatientICN") %>%
  left_join(temp_spline_summary,       by = "PatientICN") %>%
  left_join(most_recent_pulse,         by = "PatientICN") %>%
  left_join(bun_spline_summary,        by = "PatientICN") %>%
  left_join(albumin_spline_summary,    by = "PatientICN") %>%
  left_join(leukocytes_spline_summary, by = "PatientICN")

# Save combined features (M1)
write_parquet(final_vitals_features, 'data\\final_vitals_features_with_splines.parquet')

# ===========================================================================
# M2 VITALS FEATURES — anchored to index_date (ED arrival / IP admit date)
# Reuses same processing functions; omop_vitals_m2 has date_first = index_date.
# ===========================================================================
cat("\n=== Processing M2 vital signs (index_date anchor) ===\n")

weight_m2   <- process_weight(omop_vitals_m2)
height_m2   <- process_height(omop_vitals_m2)
bmi_m2      <- calculate_bmi(weight_m2, height_m2)
sbp_m2      <- process_systolic_bp(omop_vitals_m2)
dbp_m2      <- process_diastolic_bp(omop_vitals_m2)
bp_map_m2   <- calculate_map(sbp_m2, dbp_m2)
spo2_m2     <- process_spo2(omop_vitals_m2)
temp_m2     <- process_temperature(omop_vitals_m2)

bmi_traj_m2  <- get_five_year_trajectory(bmi_m2,    "bmi")
map_traj_m2  <- get_five_year_trajectory(bp_map_m2, "map")
sbp_traj_m2  <- get_five_year_trajectory(sbp_m2,    "sbp")
dbp_traj_m2  <- get_five_year_trajectory(dbp_m2,    "dbp")
spo2_traj_m2 <- get_prior_year_trajectory(spo2_m2,  "spo2")
temp_traj_m2 <- get_prior_year_trajectory(temp_m2,  "temperature")

bmi_spline_m2  <- fit_spline_summary(bmi_traj_m2,  "bmi")  %>% rename_with(~paste0("bmi_",  .), -PatientICN)
map_spline_m2  <- fit_spline_summary(map_traj_m2,  "map")  %>% rename_with(~paste0("map_",  .), -PatientICN)
sbp_spline_m2  <- fit_spline_summary(sbp_traj_m2,  "sbp")  %>% rename_with(~paste0("sbp_",  .), -PatientICN)
dbp_spline_m2  <- fit_spline_summary(dbp_traj_m2,  "dbp")  %>% rename_with(~paste0("dbp_",  .), -PatientICN)
spo2_spline_m2 <- fit_spline_summary(spo2_traj_m2, "spo2") %>% rename_with(~paste0("spo2_", .), -PatientICN)
temp_spline_m2 <- fit_spline_summary(temp_traj_m2, "temperature") %>% rename_with(~paste0("temp_", .), -PatientICN)

hr_m2 <- omop_vitals_m2 %>%
  filter(MEASUREMENT_CONCEPT_ID %in% vital_concepts$heart_rate) %>%
  filter(!is.na(VALUE_AS_NUMBER), VALUE_AS_NUMBER >= 30, VALUE_AS_NUMBER <= 300) %>%
  mutate(days_before_index = as.numeric(ymd(date_first) - ymd(MEASUREMENT_DATE)),
         heart_rate = VALUE_AS_NUMBER) %>%
  filter(days_before_index >= 0 & days_before_index <= 365) %>%
  select(PatientICN, MEASUREMENT_DATE, heart_rate, days_before_index)

pulse_m2 <- hr_m2 %>%
  group_by(PatientICN) %>%
  arrange(days_before_index) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  select(PatientICN, recent_pulse = heart_rate, pulse_days_before = days_before_index)

# M2 labs
bun_m2 <- omop_labs_m2 %>%
  filter(MEASUREMENT_CONCEPT_ID %in% lab_concepts$bun) %>%
  filter(!is.na(VALUE_AS_NUMBER), VALUE_AS_NUMBER > 0) %>%
  mutate(bun_mgdl = case_when(
    tolower(UNIT_SOURCE_VALUE) %in% c('mg/dl', 'mg/dl', 'mg/dl') ~ VALUE_AS_NUMBER,
    TRUE ~ VALUE_AS_NUMBER)) %>%
  filter(bun_mgdl >= 1 & bun_mgdl <= 200) %>%
  mutate(days_before_index = as.numeric(ymd(date_first) - ymd(MEASUREMENT_DATE)), bun = bun_mgdl) %>%
  filter(days_before_index >= 0 & days_before_index <= 365) %>%
  select(PatientICN, MEASUREMENT_DATE, bun, days_before_index)

albumin_m2 <- omop_labs_m2 %>%
  filter(MEASUREMENT_CONCEPT_ID %in% lab_concepts$albumin) %>%
  filter(!is.na(VALUE_AS_NUMBER), VALUE_AS_NUMBER > 0) %>%
  mutate(albumin_gdl = case_when(
    tolower(UNIT_SOURCE_VALUE) %in% c('g/dl', 'gm/dl') ~ VALUE_AS_NUMBER,
    tolower(UNIT_SOURCE_VALUE) %in% c('mg/dl') ~ VALUE_AS_NUMBER / 1000,
    TRUE ~ VALUE_AS_NUMBER)) %>%
  filter(albumin_gdl >= 1 & albumin_gdl <= 7) %>%
  mutate(days_before_index = as.numeric(ymd(date_first) - ymd(MEASUREMENT_DATE)), albumin = albumin_gdl) %>%
  filter(days_before_index >= 0 & days_before_index <= 365) %>%
  select(PatientICN, MEASUREMENT_DATE, albumin, days_before_index)

leukocytes_m2 <- omop_labs_m2 %>%
  filter(MEASUREMENT_CONCEPT_ID %in% lab_concepts$leukocytes) %>%
  filter(!is.na(VALUE_AS_NUMBER), VALUE_AS_NUMBER > 0) %>%
  mutate(leukocytes_kul = VALUE_AS_NUMBER) %>%
  filter(leukocytes_kul >= 0.5 & leukocytes_kul <= 100) %>%
  mutate(days_before_index = as.numeric(ymd(date_first) - ymd(MEASUREMENT_DATE)), leukocytes = leukocytes_kul) %>%
  filter(days_before_index >= 0 & days_before_index <= 365) %>%
  select(PatientICN, MEASUREMENT_DATE, leukocytes, days_before_index)

bun_traj_m2        <- get_prior_year_trajectory(bun_m2,        "bun")
albumin_traj_m2    <- get_prior_year_trajectory(albumin_m2,    "albumin")
leukocytes_traj_m2 <- get_prior_year_trajectory(leukocytes_m2, "leukocytes")

bun_spline_m2        <- fit_spline_summary(bun_traj_m2,        "bun")        %>% rename_with(~paste0("bun_",   .), -PatientICN)
albumin_spline_m2    <- fit_spline_summary(albumin_traj_m2,    "albumin")    %>% rename_with(~paste0("albumin_",.), -PatientICN)
leukocytes_spline_m2 <- fit_spline_summary(leukocytes_traj_m2, "leukocytes") %>% rename_with(~paste0("leuk_",  .), -PatientICN)

final_vitals_features_m2 <- cohort_m2 %>%
  select(PatientICN) %>%
  left_join(bmi_spline_m2,         by = "PatientICN") %>%
  left_join(map_spline_m2,         by = "PatientICN") %>%
  left_join(sbp_spline_m2,         by = "PatientICN") %>%
  left_join(dbp_spline_m2,         by = "PatientICN") %>%
  left_join(spo2_spline_m2,        by = "PatientICN") %>%
  left_join(temp_spline_m2,        by = "PatientICN") %>%
  left_join(pulse_m2,              by = "PatientICN") %>%
  left_join(bun_spline_m2,         by = "PatientICN") %>%
  left_join(albumin_spline_m2,     by = "PatientICN") %>%
  left_join(leukocytes_spline_m2,  by = "PatientICN")

write_parquet(final_vitals_features_m2, 'data\\final_vitals_features_with_splines_m2.parquet')
cat("Saved final_vitals_features_with_splines_m2.parquet:", nrow(final_vitals_features_m2), "rows\n")



