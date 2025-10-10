# pghdpred
Using patient generated health data to improve a hospitalization and mortality model

The following charts are generated with synthetic data based on the structure and dynamics of the real data.

<img width="1008" height="805" alt="image" src="https://github.com/user-attachments/assets/708d0199-1fa7-4768-b4f3-108957f5148b" />

<img width="1008" height="805" alt="image" src="https://github.com/user-attachments/assets/2ebc43ba-ec00-46c1-bed5-c7714358cb8a" />

## Synthetic Dataset Generator

This R script (`3B.1- Synthetic Dataset.R`) generates synthetic patient data that mimics the structure and patterns of real patient-generated health data (PGHD) for research and visualization purposes. The synthetic data maintains the same analytical patterns as real data while being completely artificial and safe for sharing.

### Overview

The synthetic dataset simulates:
- **1,000 patients** with realistic observation periods
- **Multiple device types** (Apple Health, Fitbit, Garmin)
- **Clinical events** (ED visits, inpatient stays, deaths)
- **Declining step patterns** leading up to clinical events
- **Device prioritization** logic for analysis

### Key Features

#### Event Distribution
- **30%** of patients have ED visits
- **10%** of patients have inpatient stays  
- **1%** of patients die
- **~40%** of deaths occur without prior ED/inpatient visits (realistic healthcare patterns)

#### Device Ownership Model
- **60%** Apple Health only
- **30%** Fitbit/Garmin only
- **10%** Both device types
- Device prioritization: Fitbit/Garmin preferred over Apple when both available

#### Temporal Patterns
- **Declining step activity** in the 10 weeks leading up to clinical events
- **Weekend effects** (20% reduction in weekend activity)
- **Event timing**: ED visits → Inpatient stays → Deaths (when multiple events occur)

## Data Dictionary: `synthetic_daily` Dataset

### Patient Identifiers
| Variable | Type | Description |
|----------|------|-------------|
| `pat_id` | character | Synthetic patient identifier (PAT_0001, PAT_0002, etc.) |

### Observation Periods
| Variable | Type | Description |
|----------|------|-------------|
| `date_first` | Date | First date of patient observation/enrollment |
| `obs_duration` | integer | Length of observation period in days (200-400 days) |
| `date_last` | Date | Last date of patient data collection |
| `obs_end` | Date | End of observation window (includes follow-up period) |
| `date` | Date | Daily observation date |

### Event Flags
| Variable | Type | Description |
|----------|------|-------------|
| `has_ed_visit` | logical | Patient had an ED visit during observation |
| `has_inpatient` | logical | Patient had an inpatient stay during observation |
| `has_death` | logical | Patient died during observation |
| `event_type` | character | Primary event type: "ed_visit", "inpatient", "death", "no_event" |
| `has_event` | logical | Patient had any clinical event (TRUE/FALSE) |

### Event Timing
| Variable | Type | Description |
|----------|------|-------------|
| `ed_visit_date` | numeric | Date of ED visit (days since 1970-01-01) |
| `inpatient_date` | numeric | Date of inpatient admission (days since 1970-01-01) |
| `death_date` | numeric | Date of death (days since 1970-01-01) |
| `event_date` | Date | Date of primary clinical event |

### Device Information
| Variable | Type | Description |
|----------|------|-------------|
| `device_profile` | character | Device ownership: "apple_only", "fit_garmin_only", "both" |
| `has_apple` | logical | Patient owns Apple Health device |
| `has_fit_garmin` | logical | Patient owns Fitbit or Garmin device |
| `primary_device` | character | Primary device when multiple owned: "apple", "fit_garmin" |

### Step Data
| Variable | Type | Description |
|----------|------|-------------|
| `ah_das_exercise_steps` | numeric | Daily steps from Apple Health (0 if no Apple device) |
| `fit_das_exercise_steps` | numeric | Daily steps from Fitbit (0 if no Fitbit device) |
| `gar_das_exercise_steps` | numeric | Daily steps from Garmin (0 if no Garmin device) |

### Synthetic Generation Variables
| Variable | Type | Description |
|----------|------|-------------|
| `base_steps` | numeric | Individual baseline step count (used for generation) |
| `day_of_week_factor` | numeric | Weekend reduction factor (0.8 weekends, 1.0 weekdays) |
| `days_to_event` | numeric | Days until clinical event (NA for no-event patients) |
| `event_decline_factor` | numeric | Step reduction factor as event approaches |

### Daily Event Indicators
| Variable | Type | Description |
|----------|------|-------------|
| `ed` | numeric | ED visit occurred on this date (1/0) |
| `inpat_any` | numeric | Inpatient admission occurred on this date (1/0) |
| `dod` | character | Date of death (if occurred on this date) |

### Additional Health Data
| Variable | Type | Description |
|----------|------|-------------|
| `ah_sleep_TOTAL_sec` | numeric | Total sleep duration in seconds (Apple Health) |
| `ah_wo_avgHR_bpm` | numeric | Average heart rate in beats per minute (Apple Health) |
| `data_day` | numeric | Indicator for days with any device data (1/0) |

## Dataset Characteristics

- **Total Records**: ~298,531 daily observations
- **Patients**: 1,000 synthetic patients
- **Time Range**: 2022-2024 (realistic enrollment dates)
- **Missing Data**: Realistic patterns of device availability and data gaps
- **Event Patterns**: Declining step activity 70 days before clinical events

## Analysis Capabilities

The synthetic dataset supports:

1. **Event-based analysis**: Step patterns leading up to clinical events
2. **Device comparison**: Apple Health vs. Fitbit/Garmin performance
3. **Temporal analysis**: Weekly and daily activity patterns
4. **Control group analysis**: Random time windows for patients without events
5. **Missing data handling**: Realistic device availability patterns

## Output

The script produces:

### Generated Datasets
- `synthetic_daily`: Primary daily-level dataset with all variables
- `steps_weekly_events`: Weekly aggregated data for event analysis
- `first_events`: Patient-level event summary

### Visualizations
1. **Main Event Plot**: Weekly step counts leading to clinical events (-10 to 0 weeks)
2. **Device Comparison Plot**: Step patterns by device type and event, faceted by event type

### Summary Statistics
- Event distribution validation
- Device ownership patterns
- Sample sizes by analysis group
- Data quality metrics

---

*This synthetic dataset is completely artificial and contains no real patient information. All patterns and relationships are algorithmically generated to mimic real-world healthcare data dynamics.*
