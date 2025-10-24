### Nick Cardamone
### OCC_PGHDPred
### 3B.2 Health factors - Create Synthetic Dataset:
### Date created: 10/22/2025
### Last updated: 10/24/2025

suppressPackageStartupMessages({
  library(dplyr)
  library(lubridate)
  library(tidyr)
  library(ggplot2)
  library(arrow)
  library(viridis)
  library(synthpop)
})

setwd("C:/Users/VHAPHICardaN/OneDrive - Department of Veterans Affairs/Desktop/Projects/OPS_Bressman-PGHDPred/")

'%!in%' <- function(x,y)!('%in%'(x,y))

cohort_full_demo <- open_dataset("parquet/cohort_full_demo.parquet")

cohort_static <- cohort_full_demo %>%
  left_join(
    open_dataset("parquet/patient_condition_flags_prior.parquet"),
    by = "PatientICN"
  ) %>% 
  left_join(
    open_dataset("parquet/inpat_prior.parquet") %>% distinct(),
    by = "PatientICN"
  ) %>% 
  left_join(
    open_dataset("parquet/ed_urgent_prior.parquet") %>% distinct(),
    by = "PatientICN"
  ) %>% 
  left_join(
    open_dataset("parquet/final_vitals_features_with_splines.parquet"),
    by = "PatientICN"
  ) %>%
  left_join(
    open_dataset("parquet/final_health_factors_tobacco_mst.parquet") %>% select(-date_first),
    by = "PatientICN"
  ) %>%
  collect()

cohort_daily <- open_dataset("parquet/pghd_final_full.parquet")
inpat_daily <- open_dataset("parquet/inpat_daily.parquet")
ed_urgent_daily <- open_dataset("parquet/ed_urgent_daily.parquet")

# Join demographic and event data
cohort_daily <- cohort_daily %>%
  left_join(cohort_full_demo, by = "PatientICN") %>%
  left_join(inpat_daily, by = c("PatientICN", "date")) %>%
  left_join(ed_urgent_daily %>% mutate(date = as.Date(date)), by = c("PatientICN", "date")) %>%
  mutate(died_in_window = if_else(as.Date(dod) <= as.Date(obs_end), 1, 0)) %>%
  filter(date_first <= date & date <= obs_end)

# Device prioritization: Combine fit/garmin as priority over apple
cohort_steps_processed <- cohort_daily %>%
  transmute(
    PatientICN,
    date, 
    date_first,
    date_last,
    obs_end,
    dod,
    ed = coalesce(ed, 0),
    inpat_any = coalesce(inpat_any, 0),
    # Prioritize Fitbit/Garmin over Apple: take max of fit/garmin, then prefer that over apple
    fit_garmin_steps = pmax(fit_das_exercise_steps, gar_das_exercise_steps, na.rm = TRUE),
    apple_steps = ah_das_exercise_steps
  ) %>%
  collect()


set.seed(4100)

# Parameters
n_patients <- 1200   # Number of synthetic patients

# Generate patient IDs
pat_id <- paste0("PAT_", sprintf("%04d", 1:n_patients))

# Generate patient-level characteristics
patient_chars <- tibble(
  pat_id = pat_id,
  date_first = sample(seq.Date(ymd("2022-01-01"), ymd("2024-06-01"), by = "day"), n_patients, replace = TRUE),
  obs_duration = sample(200:400, n_patients, replace = TRUE),
  date_last = date_first + obs_duration,
  obs_end = date_last + sample(0:30, n_patients, replace = TRUE),
  
  # Prior 2-year condition flags (py2_*)
  py2_acute_renal_failure = rbinom(n_patients, 1, 0.01571),
  py2_artificial_openings = rbinom(n_patients, 1, 0.0001595),
  py2_chronic_pancreatitis = rbinom(n_patients, 1, 0.002552),
  py2_chronic_ulcer_skin = rbinom(n_patients, 1, 0.02887),
  py2_congestive_heart_failure = rbinom(n_patients, 1, 0.03038),
  py2_drug_alcohol_dependence = rbinom(n_patients, 1, 0.04864),
  py2_drug_alcohol_psychosis = rbinom(n_patients, 1, 0.009648),
  py2_lung_severe_cancers = rbinom(n_patients, 1, 0.002631),
  py2_metastatic_cancer = rbinom(n_patients, 1, 0.001754),
  py2_heart_arrhythmias = rbinom(n_patients, 1, 0.08524),
  py2_dialysis_status = rbinom(n_patients, 1, 0.002312),
  py2_mwi_score = pmax(0, rnorm(n_patients, 8.756, 7)),
  
  # Prior 2-year inpatient flags (py2_inpat_*)
  py2_inpat_med_surg = rbinom(n_patients, 1, 0.8019),
  py2_inpat_mental_health = rbinom(n_patients, 1, 0.1472),
  py2_inpat_nursing_home = rbinom(n_patients, 1, 0.0123),
  py2_inpat_other = rbinom(n_patients, 1, 0.1943),
  
  # Prior ED/urgent care
  py_ed = rbinom(n_patients, 1, 0.9037),
  py_urgent = rbinom(n_patients, 1, 0.1304),
  
  # BMI statistics
  bmi_fitted_mean = rnorm(n_patients, 32.77, 8),
  bmi_slope = rnorm(n_patients, -0.0207, 5),
  
  # MAP (Mean Arterial Pressure) statistics
  map_fitted_mean = rnorm(n_patients, 95.99, 10),
  map_slope = rnorm(n_patients, -1.0208, 50),
  
  # Pulse
  recent_pulse = round(rnorm(n_patients, 76.44, 15)),
  
  # BUN (Blood Urea Nitrogen) statistics
  bun_fitted_mean = pmax(5, rnorm(n_patients, 19.36, 15)),
  bun_slope = rnorm(n_patients, 0.4334, 20),
  
  # Albumin statistics
  albumin_fitted_mean = pmax(2, pmin(5.5, rnorm(n_patients, 4.142, 0.4))),
  albumin_slope = rnorm(n_patients, -0.1885, 5),
  
  # Leukocytes (White Blood Cell Count) statistics
  leukocytes_fitted_mean = pmax(2, rnorm(n_patients, 7.458, 3)),
  leukocytes_slope = rnorm(n_patients, -0.5785, 20),
  
  # Tobacco status
  tobacco_status = sample(c("Current", "Former", "Never", "Unknown", NA), 
                          n_patients, replace = TRUE, 
                          prob = c(0.15, 0.25, 0.45, 0.10, 0.05)),
  
  # MST (Military Sexual Trauma) status
  mst_status = rbinom(n_patients, 1, 0.1232),
  
  # Geographic Information System Urban/Rural/Highly Rural (GISURH)
  # H = Highly Rural (0.58%), I = Insular (0.09%), R = Rural (29.37%), U = Urban (69.96%)
  GISURH = sample(c("H", "I", "R", "U", NA), 
                  n_patients, 
                  replace = TRUE,
                  prob = c(0.0058451437, 0.0008807751, 0.2936984546, 0.6995756266, 0.004146400)),
  
  # VA Priority Group (1-8, with 1 being highest priority)
  PriorityGroup = sample(c("Group 1", "Group 2", "Group 3", "Group 4", 
                           "Group 5", "Group 6", "Group 7", "Group 8", NA),
                         n_patients,
                         replace = TRUE,
                         prob = c(0.728587415, 0.063097185, 0.088577085, 0.001463373,
                                  0.037272962, 0.026943273, 0.015494534, 0.038564173, 0.073678335)),
  
  # Marital status (0 = Not Married, 1 = Married)
  Married = sample(c(0, 1, NA),
                   n_patients,
                   replace = TRUE,
                   prob = c(0.4005525, 0.5994475, 0.01857906)),
  
  # Prior 2-year Individual Veterans Care (IVC) days - Community care utilization
  # Most patients (82.7%) have 0 days, with a long tail distribution
  py2_IVC_n = sample(c(0:28, 31, 32, 35, 38, 43, 49, 53, 70),
                     n_patients,
                     replace = TRUE,
                     prob = c(
                       0.827047, 0.089467, 0.035005, 0.015230, 0.008054, 0.006299, 0.004465, 0.002073,
                       0.001914, 0.001834, 0.001675, 0.000877, 0.000638, 0.001037, 0.000877, 0.000319,
                       0.000718, 0.000239, 0.000239, 0.000159, 0.000399, 0.000239, 0.000080, 0.000080,
                       0.000080, 0.000080, 0.000080, 0.000080, 0.000080,  # 0-28
                       0.000159, 0.000080, 0.000159, 0.000080, 0.000080, 0.000080, 0.000080, 0.000080  # 31,32,35,38,43,49,53,70
                     )),
  
  # Assign specific event probabilities
  has_ed_visit = sample(c(TRUE, FALSE), n_patients, replace = TRUE, prob = c(0.30, 0.70)),
  has_inpatient = sample(c(TRUE, FALSE), n_patients, replace = TRUE, prob = c(0.12, 0.90)),
  has_death = sample(c(TRUE, FALSE), n_patients, replace = TRUE, prob = c(0.02, 0.99)),
  
  # Determine primary event type (what we'll analyze as the "event")
  # Priority: death > inpatient > ed_visit > no_event
  event_type = case_when(
    has_death ~ "death",
    has_inpatient ~ "inpatient", 
    has_ed_visit ~ "ed_visit",
    TRUE ~ "no_event"
  ),
  has_event = event_type != "no_event",
  
  # Event timing - ensure proper ordering
  # ED visits: 30-100 days after start
  ed_visit_date = ifelse(has_ed_visit, 
                         as.numeric(date_first + sample(30:100, n_patients, replace = TRUE)),
                         NA),
  # Inpatient visits: 40-110 days after start (later than ED if both occur)
  inpatient_date = ifelse(has_inpatient,
                          as.numeric(date_first + sample(pmax(40, ifelse(has_ed_visit, 
                                                                         as.numeric(as.Date(ed_visit_date, origin = "1970-01-01") - date_first) + 5, 
                                                                         40)):110, n_patients, replace = TRUE)),
                          NA),
  # Deaths: can occur anytime 50-120 days after start
  # If patient has other events, death occurs 5+ days after the latest other event
  # If no other events, death can occur anytime in the 50-120 day range
  death_date = ifelse(has_death,
                      as.numeric(date_first + sample(
                        pmax(50, 
                             ifelse(has_inpatient, 
                                    as.numeric(as.Date(inpatient_date, origin = "1970-01-01") - date_first) + 5,
                                    ifelse(has_ed_visit,
                                           as.numeric(as.Date(ed_visit_date, origin = "1970-01-01") - date_first) + 5,
                                           50))):120, n_patients, replace = TRUE)),
                      NA),
  
  # Primary event date (for analysis - the first/most severe event)
  event_date = case_when(
    event_type == "death" ~ as.Date(death_date, origin = "1970-01-01"),
    event_type == "inpatient" ~ as.Date(inpatient_date, origin = "1970-01-01"),
    event_type == "ed_visit" ~ as.Date(ed_visit_date, origin = "1970-01-01"),
    TRUE ~ as.Date(NA)
  ),
  
  # Device assignment - create realistic mix of device ownership
  # 60% Apple only, 30% Fitbit/Garmin only, 10% have both
  device_profile = sample(c("apple_only", "fit_garmin_only", "both"), n_patients, 
                          replace = TRUE, prob = c(0.60, 0.30, 0.10)),
  has_apple = device_profile %in% c("apple_only", "both"),
  has_fit_garmin = device_profile %in% c("fit_garmin_only", "both"),
  
)

# Expand to daily records (patient-level variables will be repeated for each day)
synthetic_daily <- patient_chars %>%
  rowwise() %>%
  mutate(
    dates = list(seq.Date(date_first, date_last, by = "day"))
  ) %>%
  unnest(cols = c(dates)) %>%
  ungroup() %>%
  rename(date = dates)

# Generate device-specific step data with realistic patterns
synthetic_daily <- synthetic_daily %>%
  mutate(
    # Base step count varies by individual patterns (device affects reporting, not actual activity)
    base_steps = rnorm(n(), 5000, 2000),
    
    # Add weekly patterns (lower on weekends)
    day_of_week_factor = case_when(
      weekdays(date) %in% c("Saturday", "Sunday") ~ 0.8,
      TRUE ~ 1.0
    ),
    
    # Add declining pattern for event patients
    days_to_event = ifelse(has_event, as.numeric(event_date - date), NA),
    event_decline_factor = case_when(
      !has_event ~ 1.0,
      days_to_event > 70 ~ 1.0,
      days_to_event <= 70 & days_to_event > 0 ~ 1.0 - (70 - days_to_event) * 0.01,
      days_to_event <= 0 ~ 0.3,
      TRUE ~ 1.0
    ),
    
    # Generate device-specific step columns based on device ownership
    ah_das_exercise_steps = ifelse(has_apple, 
                                   pmax(0, round(base_steps * day_of_week_factor * event_decline_factor + rnorm(n(), 0, 500))), 
                                   NA),
    fit_das_exercise_steps = ifelse(has_fit_garmin, 
                                    pmax(0, round(base_steps * day_of_week_factor * event_decline_factor + rnorm(n(), 0, 700))), 
                                    NA),
    # Some Fitbit users also have Garmin data (about 30% of fit_garmin users)
    gar_das_exercise_steps = ifelse(has_fit_garmin & runif(n()) < 0.3, 
                                    pmax(0, round(base_steps * day_of_week_factor * event_decline_factor + rnorm(n(), 0, 600))), 
                                    NA),
    
    # Generate event flags using the specific event dates
    ed = ifelse(has_ed_visit & date == as.Date(ed_visit_date, origin = "1970-01-01"), 1, 0),
    inpat_any = ifelse(has_inpatient & date == as.Date(inpatient_date, origin = "1970-01-01"), 1, 0),
    dod = ifelse(has_death & date == as.Date(death_date, origin = "1970-01-01"), as.character(date), NA),
    
    # Generate sleep and HR data
    ah_sleep_TOTAL_sec = ifelse(!is.na(ah_das_exercise_steps) & runif(n()) > 0.3, 
                                round(rnorm(n(), 22000, 4000)), NA),
    ah_wo_avgHR_bpm = ifelse(!is.na(ah_das_exercise_steps) & runif(n()) > 0.4, 
                             round(rnorm(n(), 90, 15)), NA),
    
    # Data day indicator
    data_day = ifelse(!is.na(ah_das_exercise_steps) | !is.na(fit_das_exercise_steps) | !is.na(gar_das_exercise_steps), 1, 0)
  ) %>%
  # Clean up negative values
  mutate(
    ah_das_exercise_steps = pmax(0, ah_das_exercise_steps, na.rm = TRUE),
    fit_das_exercise_steps = pmax(0, fit_das_exercise_steps, na.rm = TRUE),
    gar_das_exercise_steps = pmax(0, gar_das_exercise_steps, na.rm = TRUE),
    ah_sleep_TOTAL_sec = pmax(0, ah_sleep_TOTAL_sec, na.rm = TRUE),
    ah_wo_avgHR_bpm = pmax(40, pmin(200, ah_wo_avgHR_bpm), na.rm = TRUE)
  )

# Filter to only include data days
synthetic_daily <- synthetic_daily %>%
  filter(data_day == 1) %>% 
  mutate(met_season = traumar::season(date)) # Meteorological season.

# Event distribution:
print(table(synthetic_daily %>% distinct(pat_id, event_type) %>% pull(event_type)))
# Device ownership distribution:
print(table(synthetic_daily %>% distinct(pat_id, device_profile) %>% pull(device_profile)))
# Device data availability:
device_availability <- synthetic_daily %>% 
  distinct(pat_id, has_apple, has_fit_garmin) %>%
  summarise(
    apple_users = sum(has_apple),
    fitbit_garmin_users = sum(has_fit_garmin),
    both_devices = sum(has_apple & has_fit_garmin),
    .groups = "drop"
  )

# Verify event proportions
event_summary <- synthetic_daily %>% 
  distinct(pat_id, has_ed_visit, has_inpatient, has_death) %>%
  summarise(
    total_patients = n(),
    ed_visits = sum(has_ed_visit, na.rm = TRUE),
    inpatient_stays = sum(has_inpatient, na.rm = TRUE), 
    deaths = sum(has_death, na.rm = TRUE),
    ed_pct = round(100 * sum(has_ed_visit, na.rm = TRUE) / n(), 1),
    inpat_pct = round(100 * sum(has_inpatient, na.rm = TRUE) / n(), 1),
    death_pct = round(100 * sum(has_death, na.rm = TRUE) / n(), 1)
  )

# Verify patient-level clinical variables
clinical_summary <- synthetic_daily %>%
  distinct(pat_id, .keep_all = TRUE) %>%
  summarise(
    # Condition flags
    acute_renal_failure_pct = round(100 * mean(py2_acute_renal_failure, na.rm = TRUE), 2),
    CHF_pct = round(100 * mean(py2_congestive_heart_failure, na.rm = TRUE), 2),
    arrhythmias_pct = round(100 * mean(py2_heart_arrhythmias, na.rm = TRUE), 2),
    drug_alcohol_dep_pct = round(100 * mean(py2_drug_alcohol_dependence, na.rm = TRUE), 2),
    
    # Prior utilization
    prior_ed_pct = round(100 * mean(py_ed, na.rm = TRUE), 2),
    prior_med_surg_pct = round(100 * mean(py2_inpat_med_surg, na.rm = TRUE), 2),
    
    # Clinical measures (means)
    mean_bmi = round(mean(bmi_fitted_mean, na.rm = TRUE), 1),
    mean_map = round(mean(map_fitted_mean, na.rm = TRUE), 1),
    mean_pulse = round(mean(recent_pulse, na.rm = TRUE), 1),
    mean_bun = round(mean(bun_fitted_mean, na.rm = TRUE), 1),
    mean_albumin = round(mean(albumin_fitted_mean, na.rm = TRUE), 2),
    mean_wbc = round(mean(leukocytes_fitted_mean, na.rm = TRUE), 2),
    
    # Social history
    mst_positive_pct = round(100 * mean(mst_status, na.rm = TRUE), 2),
    tobacco_current_pct = round(100 * mean(tobacco_status == "Current", na.rm = TRUE), 2),
    
    # Demographics & Access
    urban_pct = round(100 * mean(GISURH == "U", na.rm = TRUE), 2),
    rural_pct = round(100 * mean(GISURH == "R", na.rm = TRUE), 2),
    priority_grp1_pct = round(100 * mean(PriorityGroup == "Group 1", na.rm = TRUE), 2),
    married_pct = round(100 * mean(Married == 1, na.rm = TRUE), 2),
    
    # Community care utilization
    mean_ivc_days = round(mean(py2_IVC_n, na.rm = TRUE), 1),
    median_ivc_days = median(py2_IVC_n, na.rm = TRUE),
    no_ivc_pct = round(100 * mean(py2_IVC_n == 0, na.rm = TRUE), 2)
  )

# ============================================================================
# EVENT-BASED ANALYSIS
# ============================================================================

# Device prioritization: Combine fit/garmin as priority over apple
cohort_steps_processed <- synthetic_daily %>%
  mutate(
    # Prioritize fit/garmin over apple: take max of fit/garmin, then prefer that over apple
    fit_garmin_steps = pmax(fit_das_exercise_steps, gar_das_exercise_steps, na.rm = TRUE),
    apple_steps = ah_das_exercise_steps,
    # Final step count: prefer fit/garmin if available, otherwise apple
    final_steps = coalesce(fit_garmin_steps, apple_steps),
    # Device used
    device_used = case_when(
      apple_steps > 0 ~ "apple",
      fit_garmin_steps > 0 ~ "fit_garmin",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(final_steps)) %>%
  # Remove the has_event variable to avoid conflicts with first_events
  select(-has_event, -event_type)

# Identify first event date for each patient
first_events <- cohort_steps_processed %>%
  group_by(pat_id) %>%
  dplyr::summarise(
    # Find first ED visit
    first_ed_date = if_else(max(ed, na.rm = T) > 0, min(date[ed == 1], na.rm = T), as.Date(NA)),
    # Find first inpatient visit
    first_inpat_date = if_else(max(inpat_any, na.rm = T) > 0, 
                               min(date[inpat_any == 1], na.rm = T), 
                               as.Date(NA)),
    # Find death date
    death_date = first(death_date),
    .groups = "drop"
  )  
first_events <- first_events %>%
  rowwise() %>%
  mutate(
    # Find the earliest event
    first_event_date = min(c(first_ed_date, first_inpat_date, death_date), na.rm = TRUE),
    event_type = case_when(
      is.infinite(first_event_date) ~ "no_event",
      first_event_date == first_ed_date ~ "ed_visit",
      first_event_date == first_inpat_date ~ "inpatient", 
      first_event_date == death_date ~ "death",
      TRUE ~ "unknown"
    ),
    has_event = !is.infinite(first_event_date)
  ) %>%
  ungroup()

# Ccontrol group representation
set.seed(123)
control_patients <- first_events %>%
  filter(event_type == "no_event") %>%
  slice_sample(n = min(nrow(.), max(200, sum(first_events$has_event == TRUE))))

# Update first_events to include selected controls
first_events <- bind_rows(
  first_events %>% filter(has_event == TRUE),
  control_patients
)

# Create random start weeks for control patients
control_start_weeks <- first_events %>%
  filter(has_event == FALSE) %>%
  left_join(
    cohort_steps_processed %>%
      group_by(pat_id) %>%
      summarise(
        min_date = min(date),
        max_date = max(date),
        total_weeks = floor(as.numeric(max(date) - min(date)) / 7),
        .groups = "drop"
      ),
    by = "pat_id"
  ) %>%
  filter(total_weeks >= 11) %>%
  mutate(
    random_start_week = floor(runif(n()) * pmax(1, total_weeks - 10))
  ) %>%
  select(pat_id, random_start_week)

# Event-based weekly analysis
event_analysis <- cohort_steps_processed %>%
  left_join(first_events, by = "pat_id") %>%
  left_join(control_start_weeks, by = "pat_id") %>%
  group_by(pat_id) %>%
  mutate(
    # For patients with events: calculate weeks relative to event (week 0 = week of event)
    weeks_to_event = if_else(has_event == TRUE, 
                             floor(as.numeric(date - first_event_date) / 7),
                             NA_real_),
    # For patients without events: use the pre-calculated random start week
    weeks_from_start = if_else(has_event == FALSE,
                               floor(as.numeric(date - min(date)) / 7),
                               NA_real_),
    weeks_relative = if_else(has_event == FALSE & !is.na(random_start_week),
                             weeks_from_start - random_start_week,
                             weeks_to_event)
  ) %>%
  ungroup() %>%
  # Filter to relevant time windows
  filter(
    (has_event == TRUE & weeks_to_event >= -10 & weeks_to_event <= 0) |
      (has_event == FALSE & !is.na(weeks_relative) & weeks_relative >= 0 & weeks_relative <= 10)
  ) %>%
  # Standardize the week variable for analysis
  mutate(
    analysis_week = if_else(has_event == TRUE, weeks_to_event, weeks_relative - 10)
  )

# Weekly summary for event analysis
steps_weekly_events <- event_analysis %>%
  group_by(pat_id, analysis_week, has_event, event_type, device_used) %>%
  summarise(
    weekly_steps = sum(final_steps, na.rm = TRUE),
    days_with_data = n(),
    .groups = "drop"
  ) %>%
  # Only include weeks with at least 3 days of data
  filter(days_with_data >= 3) %>%
  # Remove extreme outliers
  filter(weekly_steps / days_with_data <= 30000)

# ============================================================================
# VISUALIZATIONS
# ============================================================================

# VIZ 1: Main event-based plot
steps_weekly_summary <- steps_weekly_events %>%
  group_by(analysis_week, has_event, event_type) %>%
  summarise(
    mean_steps = mean(weekly_steps, na.rm = TRUE),
    median_steps = median(weekly_steps, na.rm = TRUE),
    n_patients = n_distinct(pat_id),
    se_steps = sd(weekly_steps, na.rm = TRUE) / sqrt(n_patients),
    .groups = "drop"
  ) %>%
  filter(n_patients >= 4)

# Main plot: Steps leading up to events
plot1 <- ggplot(steps_weekly_summary, aes(x = analysis_week, y = mean_steps, color = event_type)) +
  geom_line(size = 1.2) +
  geom_point(aes(size = n_patients), alpha = 0.7) +
  geom_errorbar(aes(ymin = mean_steps - se_steps, ymax = mean_steps + se_steps), 
                width = 0.2, alpha = 0.7) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "red", alpha = 0.7) +
  scale_x_continuous(
    breaks = -10:0,
    labels = c("-10", "-9", "-8", "-7", "-6", "-5", "-4", "-3", "-2", "-1", "0 (Event)")
  ) +
  scale_color_manual(
    values = c("ed_visit" = "#E31A1C", "inpatient" = "#FF7F00", "death" = "#1F78B4", "no_event" = "#33A02C"),
    labels = c("ed_visit" = "ED Visit", "inpatient" = "Inpatient", "death" = "Death", "no_event" = "No Event"),
    name = "Event Type"
  ) +
  scale_size_continuous(name = "# Patients", range = c(2, 8)) +
  labs(
    title = "Weekly Step Counts Leading to Clinical Events",
    subtitle = "Weeks -10 to 0 (event week) for patients with events; 10 consecutive weeks for patients without events",
    x = "Week Relative to Event (or Time Period for No-Event Group)",
    y = "Average Weekly Steps"
  ) +
  theme_hc() +
  theme(
    plot.title = element_text(size = 14, face = "bold"),
    plot.subtitle = element_text(size = 11),
    legend.position = "bottom",
    panel.grid.minor = element_blank()
  )

print(plot1)

# VIZ 2: Steps by Device Type and Event
steps_by_device <- steps_weekly_events %>%
  group_by(analysis_week, has_event, event_type, device_used) %>%
  summarise(
    mean_steps = mean(weekly_steps, na.rm = TRUE),
    n_patients = n_distinct(pat_id),
    .groups = "drop"
  ) %>%
  filter(n_patients >= 3)

plot2 <- ggplot(steps_by_device, aes(x = analysis_week, y = mean_steps, color = device_used)) +
  geom_line(size = 1) +
  geom_point(aes(size = n_patients), alpha = 0.7) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "red", alpha = 0.7) +
  facet_grid(cols = vars(event_type)) +
  scale_color_manual(
    values = c("fit_garmin" = "#2E8B57", "apple" = "#ff6347"),
    labels = c("fit_garmin" = "Fitbit/Garmin", "apple" = "Apple Health"),
    name = "Device Type"
  ) +
  scale_size_continuous(name = "n_patients", range = c(2, 8)) +
  labs(
    title = "Weekly Step Counts by Device Type and Event",
    x = "Week Relative to Event",
    y = "Average Weekly Steps"
  ) +
  theme_hc() +
  theme(legend.position = "bottom")

print(plot2)

# Take subset of patients from daily sample to create .csv

synthetic_daily_grouped <- synthetic_daily %>% 
  group_by(pat_id) %>%
  slice_sample(n=100)

write.csv(synthetic_daily_grouped, "synthetic_data.csv")
