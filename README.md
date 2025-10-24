# OPS_Bressman-PGHDPred Project

## Project Overview
This quality improvement project, funded through the **VA Office of Connected Care**, leverages increasingly available patient-generated health data (PGHD) from wearable and smart devices to make more accurate and precise predictions of Veteran hospitalization and mortality. The analysis period spans **January 1, 2022 to September 25, 2025**, with outcomes tracked through **October 24, 2025**.

## Project Objective
Take advantage of patient-generated health data (via wearable and smart devices) to make more accurate and precise predictions of Veteran hospitalization and mortality.

## Project Structure

### 01_R_Scripts/
R scripts implementing the complete data pipeline:

#### **Step 1A: Generate Cohort**
- Extract all individuals with PGHD data from `DOEx.GENERIC_PGHD`
- Categories: "Sleep", "Workout", and "Daily Activity Summary"
- Extract and clean 48 PGHD features
- Expand dataset to include all days in analysis period for each person
- Create person-level summary data (first upload date, lookback periods)
- Calculate meteorological season using `season()` function from traumar package

#### **Step 2A: Demographics**
- **Data Sources**: OMOP Person View, Spatient.Spatient, Spatient.Address, Veteran.ADRPerson, MVIPerson, OMOP.Death
- **Features Extracted**:
  - Gender, Race, Ethnicity
  - Marital status (at or before first upload date)
  - GIS-URH rurality indicator (based on address)
  - VA Priority Group enrollment status
  - Date of death (outcome variable)
  - Age (calculated from birthdate and first upload date)

#### **Step 2B: Health Care Utilization**
- **Data Sources**: Outpat.Workload, Inpat.Inpatient, IVC_CDS.CDS_Claim_Header, Appt.Appointment
- **Features Extracted**:
  - Prior year/two years: ED visits, urgent care visits, inpatient stays
  - Inpatient categorization using SpecialtyIEN and PTF Codes (HERC methodology):
    - Med/Surgery, Mental Health, Nursing Home, etc.
  - Community Care visits and length of stay (prior year)
  - No-show appointments (prior year)
- **Outcomes Generated**:
  - Person-level prior healthcare utilization
  - Person-day level ED visits during analytic window
  - Person-day level inpatient stays during analytic window

#### **Step 2C: Medical Conditions**
- **Data Sources**: OMOP Visit Occurrence
- **Features Extracted**:
  - 17 conditions from CAN model (prior two years)
  - Multimorbidity Weighted Index (MWI) using Wei et al. 2024 methodology
  - ICD10/CPT code matching for condition identification
- **Output**: Person-level prior medical conditions (parquet)

#### **Step 2D: Vital Signs and Labs**
- **Data Sources**: OMOP Measurement
- **Vital Signs** (prior 5 years):
  - BMI (from static height + most recent weight)
  - Mean Arterial Pressure (from systolic/diastolic BP)
  - Heart rate/Pulse (most recent prior year)
  - Five-year spline trajectories for BMI and MAP
- **Laboratory Values** (prior year):
  - Blood Urea Nitrogen (BUN)
  - Albumin
  - White Blood Cell count (Leukocytes)
  - LOINC codes from OMOP Cipher webpage
  - Unit standardization, outlier removal, prior-year trajectories
- **Technical Notes**: 
  - Natural splines require ≥3 day-level data points
  - Multiple same-day measurements averaged
  - Uses `ns()` from splines package

#### **Step 2E: Tobacco Use and Military Sexual Trauma**
- **Data Sources**: HF.HealthFactor, PatSub.MilitarySexualTrauma
- **Features Extracted**:
  - Smoking status (prior 5 years) using VACS lookup table
  - Most recent smoking categorization
  - MST status (any affirmation = yes)

#### **Step 3A: Visualization and Analysis**
- Analytical models and visualizations

#### **Step 3B: Create Synthetic Data**
- Generate fake data based on aggregated features of real data

### 02_SQL_Scripts/
SQL queries for data extraction from protected databases

### 03_Documentation/
- Meeting notes and project planning
- Data codebook and variable specifications
- Research protocols and methodologies

### 04_Data_Connection/
- Health factor lookup tables (VACS smoking status)
- Database connection parameters

### parquet/
Processed datasets stored in efficient parquet format

## Key Datasets Generated

| Dataset | Description | Level | Key Variables |
|---------|-------------|-------|---------------|
| **cohort_static** | Person-level baseline characteristics | Person | Demographics, prior conditions, vital trajectories, MWI score |
| **cohort_daily** | Person-day observations | Person-Day | PGHD features (steps, sleep, workouts), outcomes (ED, inpatient, death) |
| **pghd_features** | Cleaned PGHD measurements | Person-Day-Measurement | 48 features across Sleep, Workout, Activity categories |

## PGHD Features (48 Total)
- **Daily Activity Summary**: Steps, distance, active energy, exercise time, stand hours, etc.
- **Sleep**: Total sleep, REM, deep, core, awake time, heart rate metrics
- **Workout**: Duration, energy burned, average/max heart rate, distance

## Outcomes Tracked
1. **Emergency Department Visits** (person-day level during analysis window)
2. **Inpatient Hospitalizations** (person-day level during analysis window)
3. **Mortality** (date of death from OMOP.Death)

## Analysis Period
- **Cohort Definition**: January 1, 2022 - September 25, 2025
- **Outcome Tracking**: Through October 24, 2025
- **Observation Windows**: Extend 30 days after last upload date per person

## Security and Compliance
- **Protected Data Environment**: All analysis conducted behind VA firewall
- **HIPAA Compliance**: Adherence to VA data security requirements
- **Synthetic Data**: Generated for external sharing and validation

## Data Sources
- **DOEx.GENERIC_PGHD**: Patient-generated health data repository
- **OMOP CDM**: Person, Death, Measurement, Visit Occurrence
- **VA CDW**: Outpat.Workload, Inpat.Inpatient, Spatient, Veteran tables
- **Community Care**: IVC_CDS claims data
- **Clinical Data**: HF.HealthFactor, PatSub.MilitarySexualTrauma

## Collaboration
- **VA Office of Connected Care**: Project funding and support
- **OCC PGHD ML Group**: Machine learning methodology
- **Bressman Research Team**: Clinical research leadership
- **Health Economic Resource Center (HERC)**: Inpatient categorization methodology

## Technical Environment
- **Language**: R (tidyverse, arrow, data.table, splines)
- **Storage**: Parquet format for efficient large-scale data
- **Development**: VS Code with secure VA network access
