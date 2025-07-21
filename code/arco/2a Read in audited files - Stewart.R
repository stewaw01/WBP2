library(tidyverse)
library(lubridate)
library(janitor)
library(openxlsx)
library(readxl)

# Source functions script
source(paste(here::here(), "code", "arco", "arco_functions.R", sep = "/"))

# Set flag for use of synthetic data ----
## Synthetic data files have been created for submission to GCU which match
## the structure of the data in the returned files, but do not contain any patient data.

## Set TRUE or FALSE
use_synthetic = TRUE


# Set file paths ----
if (use_synthetic == TRUE) {
  audited_files_path <- paste(here::here(), "data", "synthetic_audit_data/", sep = "/")
} else {
  audited_files_path <- "/conf/bss/01-Projects/01-Open-projects/XRB12024 - Neurosurgery MSN/Master Record of ARCO Data/2023 Q2 to 2024 Q1 - Audited"
}

lookups_path <- paste(here::here(), "data", "lookups/", sep = "/")

files_to_process <- list.files(path = audited_files_path, pattern = "\\.xlsx$", full.names = TRUE, recursive = TRUE)


# Read excel files from folder ----
## Selects relevant columns as defined in functions script
all_data <- files_to_process |> 
  map(function(file_name){
    read_excel(file_name, sheet = "Data", col_types = "text", .name_repair = "universal") |> 
      clean_names() |> 
      select_relevant_cols()
  })

combined_data <- bind_rows(all_data)

# Filter spurious rows ----
## Filter rows based on plausible clinician_code format (6-8 digits and not NA)
original_row_count <- nrow(combined_data)


combined_data <- combined_data |> 
  filter(!is.na(clinician_code) & str_detect(clinician_code, "^\\d{6,8}$"))

rows_removed <- original_row_count - nrow(combined_data)
cat("Removed", rows_removed, "rows due to NA or non-compliant clinician_code format.\n")

# Clean/Convert columns ----
## Get all date columns that require cleaning by specific name
date_col_names_to_convert <- c("first_episode_dob",
                               "first_episode_admission_date",
                               "nrs_linked_death_date_of_death",
                               "audit_this_neurosurgeon_last_operation_date",
                               "audit_other_neurosurgeon_last_operation_date_1",
                               "audit_other_neurosurgeon_last_operation_date_2",
                               "audit_amended_date_of_death")

## Get all operation date columns that require cleaning (which start with 'operations_operation_date_')
operation_date_cols <- names(combined_data)[str_starts(names(combined_data), "operations_operation_date_")]

## Create one list from relevant date columns
all_date_cols_to_convert_final <- union(date_col_names_to_convert, 
                                        operation_date_cols)

## Apply date_converter function to selected date columns
combined_data <- combined_data |> 
  mutate(across(all_of(all_date_cols_to_convert_final), 
                date_converter))


## Select relevant audit columns to be converted to factor
audit_cols_to_factorise <- audit_cols[!grepl("date|amended_main_condition|amended_admission_type|other_neurosurgeon_operation_this_spell", 
                                             audit_cols,
                                             ignore.case = TRUE)]

## Apply factorise_audit_cols function to selected audit columns
combined_data <- combined_data |> 
  mutate(across(all_of(audit_cols_to_factorise), 
                factorise_audit_cols))


## Convert operations_neuro_procedure_occurred column to binary TRUE/FALSE based on common inputs
combined_data$operations_neuro_procedure_occurred <- case_when(trim_upper(combined_data$operations_neuro_procedure_occurred) %in% c("TRUE", "T", "YES", "Y", "1") ~ TRUE, 
                                                               trim_upper(combined_data$operations_neuro_procedure_occurred) %in% c("FALSE", "F", "NO", "N", "0") ~ FALSE, 
                                                               TRUE ~ NA )


## Get all character columns that require cleaning
character_cols_to_clean <- c("clinician_code",
                             "consultant_name",
                             "consultant_hb_name",
                             "link_no",
                             "first_episode_upi_number",
                             "first_episode_cis_index",
                             "first_episode_uri",
                             "first_episode_sex",
                             "first_episode_admission_type_description",
                             "first_neuro_episode_main_condition",
                             "first_neuro_episode_other_condition_1",
                             "first_neuro_episode_other_condition_2",
                             "first_neuro_episode_other_condition_3",
                             "first_neuro_episode_other_condition_4",
                             "first_neuro_episode_other_condition_5",
                             "audit_amended_main_condition",
                             "audit_date_of_death_correct",
                             "audit_amended_admission_type",
                             "audit_first_neuro_episode_main_condition_correct",
                             "audit_other_neurosurgeon_operation_this_spell_1",
                             "audit_other_neurosurgeon_operation_this_spell_2")


## Apply character_converter function to selected character columns
## Converts to character, and removes values specified in junk_values
junk_values <- c("N/A", "N.A", "9", "", "NA")

combined_data <- combined_data |> 
  mutate(across(character_cols_to_clean, 
                ~character_convert_and_clean(.x, junk_values)))


# Final Audited Columns ----
## Create final_date_of_death column
death_cols <- c("nrs_linked_death_date_of_death",
                "audit_date_of_death_correct",
                "audit_amended_date_of_death")

if (all(death_cols %in% names(combined_data))) {
  combined_data <- combined_data |> 
    mutate(final_date_of_death = if_else(audit_date_of_death_correct == "no",
                                         if_else(!is.na(audit_amended_date_of_death),
                                                 as.Date(audit_amended_date_of_death),
                                                 NA_Date_),
                                         as.Date(nrs_linked_death_date_of_death)))
  } else { 
  combined_data$final_date_of_death <- NA_Date_ 
}

## Create final_main_condition column
main_conditions_cols <- c("first_neuro_episode_main_condition",
                          "audit_first_neuro_episode_main_condition_correct",
                          "audit_amended_main_condition")

junk_values_2 <- c(NA, "N/A", "N.A", "", "9", "10", "11", "YES")

if (all(main_conditions_cols %in% names(combined_data))) {
  combined_data <- combined_data |> 
    mutate(cleaned_audit_amended_main_condition = character_convert_and_clean(audit_amended_main_condition, junk_values_2),
           final_main_condition = if_else(audit_first_neuro_episode_main_condition_correct == "NO" & !is.na(cleaned_audit_amended_main_condition),
                                          cleaned_audit_amended_main_condition,
                                          first_neuro_episode_main_condition))
  } else { 
    combined_data$final_main_condition <- NA_character_
}



## Create final_admission_type_description column
admission_type_levels <- c("Routine Admission",
                           "Urgent or Emergency Admission",
                           "Unknown")

admission_type <- c("first_episode_admission_type_description",
                    "audit_first_episode_admission_type_correct",
                    "audit_amended_admission_type")

if (all(admission_type %in% names(combined_data))) {
  combined_data <- combined_data |> 
    mutate(standardized_audit_amended_admission_type = case_when(grepl("ELECTIVE", audit_amended_admission_type, fixed = TRUE) ~ "Routine Admission",
                                                                 grepl("ROUTINE", audit_amended_admission_type, fixed = TRUE) ~ "Routine Admission",
                                                                 !is.na(audit_amended_admission_type) ~ "Unknown",
                                                                 TRUE ~ NA_character_),
           
           standardized_original_admission_type = case_when(grepl("ELECTIVE", audit_amended_admission_type, fixed = TRUE) ~ "Routine Admission",
                                                            grepl("ROUTINE", audit_amended_admission_type, fixed = TRUE) ~ "Routine Admission",
                                                            first_episode_admission_type_description == "URGENT OR EMERGENCY ADMISSION" ~ "Urgent or Emergency Admission",
                                                            !is.na(first_episode_admission_type_description) & first_episode_admission_type_description != "" ~ "Unknown",
                                                            TRUE ~ NA_character_),
           final_admission_type_description = factor(if_else(as.character(audit_first_episode_admission_type_correct) == "NO" & !is.na(standardized_audit_amended_admission_type),
                                                             standardized_audit_amended_admission_type,
                                                             standardized_original_admission_type),
                                                     levels = admission_type_levels))
  } else {
    combined_data$final_admission_type_description <- factor(NA, levels = admission_type_levels)
}


# Print diagnostic report to the console ----
diagnostic_report()



# Prepare variables for regression ----
## Read in lookup files for diagnosis groupings and comorbidities
lookup_diagnosis_groupings <- read_csv(file.path(lookups_path, "lookup_diagnosis_grouping.csv"), show_col_types = FALSE) |> 
  clean_names()

lookup_comorbidities <- read_csv(file.path(lookups_path, "lookup_comorbidities.csv"), show_col_types = FALSE) |>
  clean_names()

## Create unique_row_id column for joining operation dates
combined_data <- combined_data |> mutate(unique_row_id = dplyr::row_number())


## Derive last_operation_date_in_stay
if(length(operation_date_cols) > 0) {
  temp_op_dates_long <- combined_data |>
    select(unique_row_id, all_of(operation_date_cols)) |>
    pivot_longer(cols = all_of(operation_date_cols),
                 names_to = "operation_date_var",
                 values_to = "operation_date",
                 values_drop_na = TRUE) |>
    filter(!is.na(operation_date))
  
  if(nrow(temp_op_dates_long) > 0) {
    last_op_dates <- temp_op_dates_long |> 
      group_by(unique_row_id) |>
      summarise(last_operation_date_in_stay = max(operation_date, na.rm = TRUE), .groups = "drop") |>
      mutate(last_operation_date_in_stay = if_else(is.infinite(last_operation_date_in_stay),
                                                   NA_Date_,
                                                   last_operation_date_in_stay))
    
    combined_data <- combined_data |> 
      left_join(last_op_dates, by = "unique_row_id")
  }
  else {
    combined_data$last_operation_date_in_stay <- NA_Date_
  }
}


## Derive age_on_admission and age_group
combined_data <- combined_data |>
  mutate(age_on_admission = floor(as.numeric(interval(first_episode_dob, first_episode_admission_date), "years")),
         
         age_group = case_when(is.na(age_on_admission) ~ NA_character_,
                               age_on_admission >= 0 & age_on_admission <= 16 ~ "Children (0-16)",
                               age_on_admission >= 17 & age_on_admission <= 64 ~ "Adults (17-64)",
                               age_on_admission >= 65 ~ "Older Adults (65+)",
                               TRUE ~ NA_character_))


## Derive diagnosis_grouping
combined_data <- combined_data |>
  mutate(main_condition_temp = toupper(substr(final_main_condition, 1, 3))) |> 
  left_join(lookup_diagnosis_groupings, by = c("main_condition_temp" = "icd10")) |>
  mutate(diagnosis_grouping = if_else(is.na(diagnosis_grouping),
                                      "other",
                                      diagnosis_grouping)) |>
  select(-main_condition_temp)


## Derive comorbidity_score and comorbidity_category
comorbidity_scores <- data.frame(comorbidity_category = c("Comorbidity1",
                                                          "Comorbidity2",
                                                          "Comorbidity3",
                                                          "Comorbidity4",
                                                          "Comorbidity5",
                                                          "Comorbidity6",
                                                          "Comorbidity7",
                                                          "Comorbidity8",
                                                          "Comorbidity9",
                                                          "Comorbidity10",
                                                          "Comorbidity11",
                                                          "Comorbidity12",
                                                          "Comorbidity13",
                                                          "Comorbidity14",
                                                          "Comorbidity15",
                                                          "Comorbidity16",
                                                          "Comorbidity17"),
                                    comorbidity_score_value = c(5,11,13,4,14,3,8,9,6,4,8,-1,1,10,14,18,2))


comorbidity_mapping <- lookup_comorbidities |>
  left_join(comorbidity_scores, by = "comorbidity_category") |>
  select(icd10, comorbidity_score_value) |> 
  distinct() |> 
  filter(!is.na(icd10) & !is.na(comorbidity_score_value)) |> 
  deframe()


combined_data <- combined_data |>
  rowwise() |> 
  mutate(comorbidity_score = sum(get_comorbidity_scores(final_main_condition),
                                 get_comorbidity_scores(first_neuro_episode_other_condition_1),
                                 get_comorbidity_scores(first_neuro_episode_other_condition_2),
                                 get_comorbidity_scores(first_neuro_episode_other_condition_3),
                                 get_comorbidity_scores(first_neuro_episode_other_condition_4),
                                 get_comorbidity_scores(first_neuro_episode_other_condition_5),
                                 na.rm = TRUE)) |>
  ungroup() |> 
  get_comorbidity_categories()


## Derive death_within_30_days_of_procedure
combined_data <- combined_data |>
  mutate(days_to_death = as.numeric(final_date_of_death - last_operation_date_in_stay),
         death_within_30_days_of_procedure = case_when(is.na(days_to_death) ~ FALSE,
                                                       days_to_death >= 0 & days_to_death <= 30 ~ TRUE,
                                                       TRUE ~ FALSE)) |>
  select(-days_to_death)


# Prepare model_input_data_audited ----
model_input_data_audited <- combined_data |>
  filter(operations_neuro_procedure_occurred == TRUE) |> 
  filter(!is.na(first_episode_sex) & first_episode_sex %in% c("1", "2")) |> 
  group_by(link_no, consultant_name) |> 
  summarise(op_date = if_else(all(is.na(last_operation_date_in_stay)), NA_Date_, max(last_operation_date_in_stay, na.rm = TRUE)),
            age_group = first(na.omit(age_group)),
            sex = first(na.omit(first_episode_sex)),
            type_of_admission_first_episode = first(na.omit(as.character(final_admission_type_description))),
            diagnosis_grouping = first(na.omit(as.character(diagnosis_grouping))),
            comorbidity_category = if_else(all(is.na(comorbidity_category)),
                                           NA_real_,
                                           suppressWarnings(max(comorbidity_category,na.rm = TRUE))),
            death_within_30_days_of_procedure = any(death_within_30_days_of_procedure == TRUE, na.rm = FALSE),
            .groups = "drop") |>
  mutate(age_group = factor(age_group, 
                            levels = c("Adults (17-64)", "Children (0-16)", "Older Adults (65+)")),
         sex = factor(sex,
                      levels = c("1", "2")),
         type_of_admission_first_episode = factor(type_of_admission_first_episode,
                                                  levels = c("Routine Admission", "Urgent or Emergency Admission", "Unknown")),
         diagnosis_grouping = factor(diagnosis_grouping),
         comorbidity_category = factor(comorbidity_category,
                                       levels = c("1", "2", "3", "4")),
         death_within_30_days_of_procedure = as.logical(death_within_30_days_of_procedure))


## Check each factor column has the correct entries and relevel
model_input_data_audited <- model_input_data_audited |> 
  check_and_relevel("age_group", "Adults (17-64)") |> 
  check_and_relevel("sex", "1")


### Check 'other' is correctly set as a factor level if it appears in diagnosis_grouping
current_diag_levels <- levels(model_input_data_audited$diagnosis_grouping)

if (!("other" %in% current_diag_levels) && any(model_input_data_audited$diagnosis_grouping == "other", na.rm=TRUE)) { 
  levels(model_input_data_audited$diagnosis_grouping) <- c(current_diag_levels, "other")
}
rm(current_diag_levels)

model_input_data_audited <- check_and_relevel(model_input_data_audited, "diagnosis_grouping", "other")


### Check levels 1, 2, 3, and 4 are correctly set as factor levels in comorbidity_category
current_comorb_levels <- levels(model_input_data_audited$comorbidity_category)
expected_comorb_levels <- c("1", "2", "3", "4")
missing_comorb_levels <- setdiff(expected_comorb_levels, current_comorb_levels)

if(length(missing_comorb_levels) > 0) {
  levels(model_input_data_audited$comorbidity_category) <- c(current_comorb_levels, missing_comorb_levels)
}
rm(current_comorb_levels, expected_comorb_levels, missing_comorb_levels)

model_input_data_audited <- check_and_relevel(model_input_data_audited, "comorbidity_category", "1")


### Check "Routine Admission" is correctly set as a factor level in type_of_admission_first_episode
current_admission_levels <- levels(model_input_data_audited$type_of_admission_first_episode)

if (!("Routine Admission" %in% current_admission_levels) && any(model_input_data_audited$type_of_admission_first_episode == "Routine Admission", na.rm=TRUE)) { 
  levels(model_input_data_audited$type_of_admission_first_episode) <- c(current_admission_levels, "Routine Admission")
}
rm(current_admission_levels)

model_input_data_audited <- check_and_relevel(model_input_data_audited, "type_of_admission_first_episode", "Routine Admission")




