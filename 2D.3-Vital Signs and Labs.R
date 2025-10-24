### Nick Cardamone
### OCC_PGHDPred
### 3. Vital signs and anthropometric measurements
### Date created: 4/29/2025
### Last updated: 10/22/2025

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
setwd("C:\\Users\\VHAPHICardaN\\OneDrive - Department of Veterans Affairs\\Desktop\\Projects\\OPS_Bressman-PGHDPred\\")

# Cohort: contains PatientICN and upload window variables (e.g. date_first)
cohort <- open_dataset('parquet\\pghd_final_full_visits_ids.parquet') %>%
  collect()

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
  weight = c(3013762, 3003176, 3025315, 3023166, 3026600),  # Body weight concepts
  height = c(3023540, 3019171, 3036277),  # Body height concepts  
  systolic_bp = c(3004249, 3018586, 3028737),  # Systolic blood pressure
  diastolic_bp = c(3012888, 3034703, 3019962),  # Diastolic blood pressure
  heart_rate = c(3027018, 3027598, 3018567),  # Heart rate/pulse
)

# Get OMOP concept metadata
omop_concept <- tbl(cdwwork, in_schema('OMOPV5', 'CONCEPT')) %>% 
  filter(CONCEPT_ID %in% !!unlist(vital_concepts)) %>%
  select(CONCEPT_ID, CONCEPT_NAME, DOMAIN_ID) %>%
  collect()

# Extract vital signs measurements for cohort (prior 5 years lookback)
omop_vitals <- tbl(cdwwork, in_schema('OMOPV5', 'MEASUREMENT')) %>% 
  inner_join(omop_xw, by = "PERSON_ID") %>% 
  filter(MEASUREMENT_CONCEPT_ID %in% !!unlist(vital_concepts)) %>%
  filter(MEASUREMENT_DATE >= five_years_prior_date & MEASUREMENT_DATE <= date_first) %>%  # 5 years prior
  select(PatientICN, PERSON_ID, MEASUREMENT_CONCEPT_ID, MEASUREMENT_DATE, 
         VALUE_AS_NUMBER, UNIT_CONCEPT_ID, UNIT_SOURCE_VALUE, date_first) %>%
  collect()

# Join with concept names for interpretation
omop_vitals <- omop_vitals %>%
  left_join(omop_concept, by = c("MEASUREMENT_CONCEPT_ID" = "CONCEPT_ID"))

# Save raw vitals data
write_parquet(omop_vitals, 'parquet/omop_vitals_raw.parquet')
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
write_parquet(bmi_calculated, 'parquet/bmi_calculated.parquet')

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
write_parquet(bp_map_calculated, 'parquet/bp_map_calculated.parquet')

# ---------------------------------------------------------------------------
# 5) Create 5-year trajectory datasets for spline modeling
# ---------------------------------------------------------------------------

# Function to prepare 5-year trajectory data for splines
# Averages multiple measurements per day before creating trajectory
get_five_year_trajectory <- function(data, value_col, patient_col = "PatientICN") {
  data %>%
    filter(days_before_index >= 0 & days_before_index <= 1825) %>%  # Within 5 years prior
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
    filter(total_measurements >= 3) %>%  # Require at least 3 measurements for spline
    ungroup() %>%
    select(!!sym(patient_col), days_before_index, MEASUREMENT_DATE, !!sym(value_col), 
           measurement_number, total_measurements)
}

# Create BMI 5-year trajectory for spline modeling
bmi_trajectory_5yr <- get_five_year_trajectory(bmi_calculated, "bmi")

# Create MAP 5-year trajectory for spline modeling
map_trajectory_5yr <- get_five_year_trajectory(bp_map_calculated, "map")

# Save trajectory datasets
write_parquet(bmi_trajectory_5yr, 'parquet/bmi_trajectory_5yr.parquet')
write_parquet(map_trajectory_5yr, 'parquet/map_trajectory_5yr.parquet')

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
  
  # Fit spline for each patient and extract summary statistics
  spline_summaries <- trajectory_data %>%
    group_by(PatientICN) %>%
    summarise(
      n_measurements = n(),
      timespan_days = max(days_before_index) - min(days_before_index),
      
      # Raw value statistics
      value_min = min(!!sym(value_col)),
      value_max = max(!!sym(value_col)),
      value_mean = mean(!!sym(value_col)),
      value_sd = sd(!!sym(value_col)),
      value_range = max(!!sym(value_col)) - min(!!sym(value_col)),
      
      # First and last measurements (chronologically correct)
      value_first = {
        data <- cur_data()
        data[[value_col]][which.max(data$days_before_index)]  # Oldest (furthest back)
      },
      value_last = {
        data <- cur_data()
        data[[value_col]][which.min(data$days_before_index)]  # Most recent (closest to index)
      },
      
      # Calculate slope (change over time)
      slope = {
        data <- cur_data()
        ts <- max(data$days_before_index) - min(data$days_before_index)
        if(ts > 0) {
          (data[[value_col]][which.min(data$days_before_index)] - 
             data[[value_col]][which.max(data$days_before_index)]) / ts * 365
        } else {
          NA_real_
        }
      },
      
      # Fit natural spline with 3 df and extract fitted values
      spline_fit = list({
        data <- cur_data()
        if(nrow(data) >= 3) {
          tryCatch({
            # Fit natural spline
            spline_model <- lm(reformulate("ns(days_before_index, df = 3)", value_col), 
                               data = data)
            
            # Extract fitted values
            fitted_vals <- fitted(spline_model)
            
            # Get fitted values at specific time points (by index in sorted data)
            fitted_first_idx <- which.max(data$days_before_index)  # Oldest
            fitted_last_idx <- which.min(data$days_before_index)   # Most recent
            
            # Return summary of fitted values
            list(
              fitted_min = min(fitted_vals),
              fitted_max = max(fitted_vals),
              fitted_mean = mean(fitted_vals),
              fitted_sd = sd(fitted_vals),
              fitted_range = max(fitted_vals) - min(fitted_vals),
              fitted_first = fitted_vals[fitted_first_idx],  # Fitted value at oldest measurement
              fitted_last = fitted_vals[fitted_last_idx]      # Fitted value at most recent measurement
            )
          }, error = function(e) {
            list(fitted_min = NA, fitted_max = NA, fitted_mean = NA, 
                 fitted_sd = NA, fitted_range = NA, fitted_first = NA, fitted_last = NA)
          })
        } else {
          list(fitted_min = NA, fitted_max = NA, fitted_mean = NA, 
               fitted_sd = NA, fitted_range = NA, fitted_first = NA, fitted_last = NA)
        }
      }),
      .groups = 'drop'
    )
  
  # Unnest the spline fit results
  spline_summaries <- spline_summaries %>%
    mutate(
      fitted_min = sapply(spline_fit, function(x) x$fitted_min),
      fitted_max = sapply(spline_fit, function(x) x$fitted_max),
      fitted_mean = sapply(spline_fit, function(x) x$fitted_mean),
      fitted_sd = sapply(spline_fit, function(x) x$fitted_sd),
      fitted_range = sapply(spline_fit, function(x) x$fitted_range),
      fitted_first = sapply(spline_fit, function(x) x$fitted_first),
      fitted_last = sapply(spline_fit, function(x) x$fitted_last)
    ) %>%
    select(-spline_fit)
  
  return(spline_summaries)
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
write_parquet(bmi_spline_summary, 'parquet/bmi_spline_summary.parquet')
write_parquet(map_spline_summary, 'parquet/map_spline_summary.parquet')

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
write_parquet(most_recent_pulse, 'parquet/most_recent_pulse.parquet')

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

# Extract lab measurements for cohort (prior year only for splines)
omop_labs <- tbl(cdwwork, in_schema('OMOPV5', 'MEASUREMENT')) %>% 
  inner_join(omop_xw, by = "PERSON_ID") %>% 
  filter(MEASUREMENT_CONCEPT_ID %in% !!unlist(lab_concepts)) %>%
  filter(MEASUREMENT_DATE >= one_years_prior_date & MEASUREMENT_DATE <= date_first) %>%  # within 1 year
  select(PatientICN, PERSON_ID, MEASUREMENT_CONCEPT_ID, MEASUREMENT_DATE, 
         VALUE_AS_NUMBER, UNIT_CONCEPT_ID, UNIT_SOURCE_VALUE, date_first) %>%
  collect()

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

# Save lab trajectory and spline data
write_parquet(bun_trajectory_1yr, 'parquet/bun_trajectory_1yr.parquet')
write_parquet(albumin_trajectory_1yr, 'parquet/albumin_trajectory_1yr.parquet')
write_parquet(leukocytes_trajectory_1yr, 'parquet/leukocytes_trajectory_1yr.parquet')

write_parquet(bun_spline_summary, 'parquet/bun_spline_summary.parquet')
write_parquet(albumin_spline_summary, 'parquet/albumin_spline_summary.parquet')
write_parquet(leukocytes_spline_summary, 'parquet/leukocytes_spline_summary.parquet')

# ---------------------------------------------------------------------------
# 10) Create final combined feature dataset
# ---------------------------------------------------------------------------

# Combine all features
final_vitals_features <- cohort %>%
  select(PatientICN, date_first) %>%
  left_join(bmi_spline_summary, by = "PatientICN") %>%
  left_join(map_spline_summary, by = "PatientICN") %>%
  left_join(most_recent_pulse, by = "PatientICN") %>%
  left_join(bun_spline_summary, by = "PatientICN") %>%
  left_join(albumin_spline_summary, by = "PatientICN") %>%
  left_join(leukocytes_spline_summary, by = "PatientICN")

# Save combined features
write_parquet(final_vitals_features, 'parquet/final_vitals_features_with_splines.parquet')



