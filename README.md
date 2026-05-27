# OPS_Bressman-PGHDPred Project

## Project Overview
This quality improvement project, funded through the **VA Office of Connected Care**, leverages patient-generated health data (PGHD) from wearable and smart devices to make more accurate and precise predictions of Veteran hospitalization and mortality. Data come from the **VA Wearable Technology Program**, in which Veterans voluntarily enrolled and shared device data (Fitbit, Garmin, or Apple Health) from January 1, 2022 through April 14, 2026.

## Project Objective
Take advantage of patient-generated health data (via wearable and smart devices) to make more accurate and precise predictions of Veteran hospitalization and mortality.

---

## Methods

### Data
Daily step counts from Fitbit and Garmin devices were prioritized (using the higher value when both were present); Apple Health data served as an alternative when Fitbit/Garmin data were unavailable. A total of 7,158,421 patient-days had usable step data; Fitbit/Garmin data were available for 11.5% of days and Apple Health for the remainder. Daily step counts of zero were treated as missing, and outliers were removed using the 1.5×IQR method applied to raw device columns. Where step data were missing but exercise duration (minutes) was recorded, steps were imputed from a linear regression model fit on matched days (`steps ~ exercise_minutes`).

### Cohort Derivation
The analytic cohort was drawn from Veterans enrolled in the VA Wearable Technology Program with a first PGHD upload between January 1, 2022 and March 16, 2025 (N = 13,184). The upper enrollment bound ensures at least 52 weeks of potential observation remain before the data extract end date, providing a uniform minimum observation window per patient. Calendar weeks are indexed relative to each patient's actual first upload date (week 1 = days 1–7, week 2 = days 8–14, etc.).

Patients were retained if they had:
- At least one qualifying week in the **baseline period** (calendar weeks 1–6; days 1–42 after first upload)
- At least one qualifying **observation week** (week 7 onward)

Patients whose composite event (ED visit, inpatient admission, or death) occurred within the first 42 days (weeks 1–6) were excluded, as no event-free anchor period exists for them. Observation windows were censored at first composite event or 52 weeks from first upload, whichever came first. The **final analytic cohort** included **11,833 Veterans** contributing **375,857 person-weeks** of observation (weeks 7+).

### Step Feature Construction
At each observation week, step-derived features were computed from 7-day, 14-day, and 21-day rolling windows anchored to the calendar week end-date:

| Feature | Description |
|---------|-------------|
| `k_steps_7d_avg` | 7-day rolling average daily step count (thousands) |
| `k_steps_7d_sd` | 7-day rolling standard deviation |
| `k_steps_7d_miss_days` | Missing days in the 7-day window |
| `low_activity_days_7d` | Days with fewer than 1,000 steps in the 7-day window |
| `k_steps_7d_avg_pctdiff` | % change from prior three 7-day window average (winsorized ±500%) |
| `k_steps_7d_pct_from_baseline` | % change from 6-week baseline average |
| `k_steps_14d_avg`, `k_steps_14d_sd` | 14-day rolling average and SD |
| `k_steps_21d_avg`, `k_steps_21d_sd` | 21-day rolling average and SD |
| `baseline_k_steps_avg` | Patient's mean daily steps across baseline weeks 1–6 (between-person anchor) |
| `baseline_miss_days_avg` | Patient's mean missing days across baseline weeks 1–6 |

These features implement a **Mundlak-style decomposition**: `baseline_k_steps_avg` captures stable individual-level activity capacity (between-person), while `k_steps_7d_pct_from_baseline` and `k_steps_7d_avg_pctdiff` capture within-person deviations from that anchor over medium and short horizons. Because the baseline is computed exclusively from pre-observation data (weeks 1–6), all deviation features are computable at any future prediction point without requiring knowledge of future step counts—a prerequisite for prospective deployment.

### Outcome Definition
The primary outcome was a **composite of ED visit, inpatient admission, or death** occurring within the prediction horizon (h = 14 or 30 days) following a given observation week's end date. The outcome was labeled positive (Y=1) if any component occurred within 1 to h days, and negative (Y=0) otherwise. Observation weeks for non-event patients were excluded if fewer than h days remained before the end of the observation window, ensuring outcome classification could be confirmed over the full horizon. This means 14-day models retain more person-weeks than 30-day models for the same patients.

### Statistical Analysis
Three feature sets were evaluated:
1. **Step-derived features only** (baseline + dynamic, n = 14 features)
2. **Static clinical features only** (demographics, comorbidity flags, prior healthcare utilization, vitals, labs, social history)
3. **Combined** (step + clinical)

For each feature set × prediction horizon (14-day, 30-day), two algorithm types were trained:
- **Penalized logistic regression** (elastic-net preprocessing)
- **Gradient-boosted trees** (XGBoost)

Patients were randomly assigned to training (70%) and testing (30%) sets with all observation-weeks for a given patient held in the same partition. To address class imbalance, logistic regression models used patient-level 3:1 undersampling; XGBoost used the full training data with `scale_pos_weight` set to the true negative-to-positive ratio.

**Calibration**: Predicted probabilities were calibrated using Platt scaling (logistic regression of observed outcomes on raw model log-odds), fit on a held-out 20% of training patients and applied to the independent test set.

**Performance metrics** (computed on Platt-calibrated test-set probabilities):
- AUC-ROC
- PR-AUC (Area Under the Precision-Recall Curve)
- Post-calibration slope (target: 1.0)
- Brier Skill Score (BSS = 1 − Brier/Brier_null, where Brier_null = p̄(1−p̄)); BSS = 0 indicates no improvement over a null model; BSS = 1 indicates perfect probabilistic skill

**Data quality stratification**: Test-set predictions were stratified post-hoc by weekly data quality—"clean" weeks (0–3 missing days; >4 days with step data) vs. "sparse" weeks (4–6 missing days; 1–3 days with step data)—to evaluate whether the model captures predictive signal even during weeks with low device engagement.

---

## Repository Structure

| File | Description |
|------|-------------|
| `1_generate_cohort.R` | Extract cohort from `DOEx.GENERIC_PGHD`; clean 48 PGHD features; expand to person-day level; compute baseline summaries and rolling step features |
| `2_demographics.R` | Extract demographics from OMOP, Spatient, Veteran tables (gender, race, ethnicity, rurality, VA priority group, age, date of death) |
| `3_outcome_health_care_utilization.R` | Extract prior and during-window ED visits, inpatient stays, community care, no-shows from Outpat, Inpat, IVC_CDS, and Appt tables |
| `4_medical_conditions.R` | Extract 17 CAN model conditions and Multimorbidity Weighted Index (MWI) from OMOP Visit Occurrence |
| `5_vitals_labs.R` | Extract vitals (BMI, MAP, heart rate) and labs (BUN, albumin, WBC) with spline trajectories from OMOP Measurement |
| `6_health_factors.R` | Extract smoking status (VACS lookup table) and Military Sexual Trauma (MST) status |
| `model_analysis_writeup_5_26_2026.Rmd` | Full model analysis, results, and manuscript-ready write-up |
| `PGHD Pred - Codebook.xlsx` | Data codebook and variable specifications |
| `synthetic_data.csv` | Synthetic dataset generated from aggregated features of real data for sharing/testing |

---

## Key Datasets Generated

| Dataset | Description | Level | Key Variables |
|---------|-------------|-------|---------------|
| **cohort_static** | Person-level baseline characteristics | Person | Demographics, prior conditions, vital trajectories, MWI score, baseline step features |
| **cohort_weekly** | Person-week observations | Person-Week | Rolling step features, composite outcome flags (14-day, 30-day) |
| **pghd_features** | Cleaned PGHD measurements | Person-Day-Measurement | 48 features across Sleep, Workout, Activity categories |

---

## PGHD Features (48 Total)
- **Daily Activity Summary**: Steps, distance, active energy, exercise time, stand hours, etc.
- **Sleep**: Total sleep, REM, deep, core, awake time, heart rate metrics
- **Workout**: Duration, energy burned, average/max heart rate, distance

---

## Outcomes Tracked
1. **Emergency Department Visits**
2. **Inpatient Hospitalizations**
3. **Mortality** (date of death from OMOP.Death)

Composite outcome (any of the above) evaluated at **14-day** and **30-day** prediction horizons.

---

## Analysis Period
- **Data Extract**: January 1, 2022 – April 14, 2026
- **Enrollment Window**: January 1, 2022 – March 16, 2025 (ensures ≥52 weeks of observation)
- **Observation Window**: Weeks 7+ after first upload, censored at first composite event or 52 weeks

---

## Data Sources
- **DOEx.GENERIC_PGHD**: Patient-generated health data repository (Fitbit, Garmin, Apple Health)
- **OMOP CDM**: Person, Death, Measurement, Visit Occurrence
- **VA CDW**: Outpat.Workload, Inpat.Inpatient, Spatient, Veteran tables
- **Community Care**: IVC_CDS claims data
- **Clinical Data**: HF.HealthFactor, PatSub.MilitarySexualTrauma

---

## Collaboration
- **VA Office of Connected Care**: Project funding and support

---

## Technical Environment
- **Language**: R (tidyverse, arrow, data.table, splines, xgboost, glmnet)
- **Storage**: Parquet format for efficient large-scale data processing
