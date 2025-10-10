
library(dplyr)
library(lubridate)
library(tidyr)
library(ggplot2)
library(viridis)

'%!in%' <- function(x,y)!('%in%'(x,y))

set.seed(12)

# Parameters
n_patients <- 1000   # Number of synthetic patients

# Generate patient IDs
pat_id <- paste0("PAT_", sprintf("%04d", 1:n_patients))

# Generate patient-level characteristics
patient_chars <- tibble(
  pat_id = pat_id,
  date_first = sample(seq.Date(ymd("2022-01-01"), ymd("2024-06-01"), by = "day"), n_patients, replace = TRUE),
  obs_duration = sample(200:400, n_patients, replace = TRUE),
  date_last = date_first + obs_duration,
  obs_end = date_last + sample(0:30, n_patients, replace = TRUE),
  
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
  
  # Primary device for step calculation (used when both devices present)
  primary_device = case_when(
    device_profile == "apple_only" ~ "apple",
    device_profile == "fit_garmin_only" ~ "fit_garmin", 
    device_profile == "both" ~ "fit_garmin"  # Prefer fit/garmin when both available
  )
)

# Expand to daily records
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
  filter(data_day == 1)

print("=== SYNTHETIC DATA SUMMARY ===")
print(paste("Total patients:", n_distinct(synthetic_daily$pat_id)))
print(paste("Total daily records:", nrow(synthetic_daily)))
print("Event distribution:")
print(table(synthetic_daily %>% distinct(pat_id, event_type) %>% pull(event_type)))
print("Device ownership distribution:")
print(table(synthetic_daily %>% distinct(pat_id, device_profile) %>% pull(device_profile)))
print("Device data availability:")
device_availability <- synthetic_daily %>% 
  distinct(pat_id, has_apple, has_fit_garmin) %>%
  summarise(
    apple_users = sum(has_apple),
    fitbit_garmin_users = sum(has_fit_garmin),
    both_devices = sum(has_apple & has_fit_garmin),
    .groups = "drop"
  )
print(device_availability)

# Verify event proportions
cat("\n=== EVENT PROPORTIONS VERIFICATION ===\n")
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
print(event_summary)

# ============================================================================
# EVENT-BASED ANALYSIS (Mimicking your original plotting script)
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

# Ensure we have adequate control group representation
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
  filter(total_weeks >= 10) %>%
  mutate(
    random_start_week = floor(runif(n()) * pmax(1, total_weeks - 9))
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
      (has_event == FALSE & !is.na(weeks_relative) & weeks_relative >= 0 & weeks_relative <= 9)
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
# RECREATE THE GRAPHS
# ============================================================================

# GRAPH 1: Main event-based plot
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
  theme_minimal() +
  theme(
    plot.title = element_text(size = 14, face = "bold"),
    plot.subtitle = element_text(size = 11),
    legend.position = "bottom",
    panel.grid.minor = element_blank()
  )

print(plot1)

# GRAPH 2: Steps by Device Type and Event
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
    values = c("fit_garmin" = "#2E8B57", "apple" = "#FF6347"),
    labels = c("fit_garmin" = "Fitbit/Garmin", "apple" = "Apple Health"),
    name = "Device Type"
  ) +
  scale_size_continuous(name = "n_patients", range = c(2, 8)) +
  labs(
    title = "Weekly Step Counts by Device Type and Event",
    x = "Week Relative to Event",
    y = "Average Weekly Steps"
  ) +
  theme_minimal() +
  theme(legend.position = "bottom")

print(plot2)

# Print summary statistics
cat("\n=== ANALYSIS SUMMARY ===\n")
print("Event counts:")
print(table(first_events$event_type))
print("\nSample sizes by event type:")
print(steps_weekly_events %>% group_by(event_type) %>% summarise(n_patients = n_distinct(pat_id), .groups = "drop"))

cat("\n=== SYNTHETIC DATA GENERATION COMPLETE ===\n")
cat("Generated", nrow(synthetic_daily), "daily records for", n_distinct(synthetic_daily$pat_id), "patients\n")
cat("Event-based analysis includes declining step patterns leading to events\n")
cat("Device model: 60% Apple only, 30% Fitbit/Garmin only, 10% have both devices\n")
cat("Device prioritization: Fitbit/Garmin preferred over Apple when both available\n")
cat("Deaths without prior healthcare events: ~40% of deaths occur without ED/inpatient visits\n")
