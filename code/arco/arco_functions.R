# Functions for script '1a extract and produce arco reports' ----
## Function to construct file name for openxlsx output ----
construct_file_name <- function(df, clinician) {
  hb_name <- df$consultant_hb_name[1]
  consultant_name <- df$consultant_name[1]
  
  file_name = paste0(output_path, "/", hb_name, "_", clinician, "_", consultant_name, "_", start_date, "-", end_date,"-test", ".xlsx")
}


## Function to apply color-coding styles to headers based on column prefixes ----
apply_style <- function(style_object, style_name, workbook, df) {
  cols_to_style <- which(startsWith(names(df), style_name))
  if (length(cols_to_style) > 0){
    addStyle(workbook,
             "Data",
             style = style_object,
             rows = 1,
             cols = cols_to_style,
             gridExpand = TRUE)
  }
}


## Function to apply validation to binary audit columns ----
apply_binary_validation <- function(value, df, workbook) {
  validation_col <- which(names(df) %in% value)
  if (length(validation_col) > 0){
    max_rows <- 1000 ### Limit to 1000 rows for efficiency
    dataValidation(workbook,
                   "Data",
                   rows = 2:max_rows,
                   cols = validation_col,
                   type = "list",
                   value = "'Yes No Dropdown'!$A$2:$A$4",
                   allowBlank = TRUE)
  }
}


## Function to apply validation to date audit columns ----
apply_date_validation <- function(value, df, workbook) {
  validation_col <- which(names(df) %in% value)
  if (length(validation_col) > 0){
    max_rows <- 1000 ### Limit to 1000 rows for efficiency
    dataValidation(workbook, 
                   "Data", 
                   rows = 2:max_rows, 
                   cols = validation_col, 
                   type = "date", 
                   operator = "greaterThan", 
                   value = as_date("2020-01-01"), 
                   allowBlank = TRUE)
  }
}


## Function to apply validation to neurosurgeon columns ----
apply_neurosurgeon_validation <- function(value, df, workbook) {
  validation_col <- which(names(df) %in% value)
  if (length(validation_col) > 0){
    max_rows <- 1000 ### Limit to 1000 rows for efficiency
    dataValidation(workbook,
                   "Data",
                   rows = 2:max_rows,
                   cols = validation_col,
                   type = "list",
                   value = paste0("'Consultant Dropdown'!$A$2:$A$", length(lookup_consultants$gmc)+1),
                   allowBlank = TRUE)
  }
}


# Functions for script '2a Read in audited files' ----
## Columns to select from audited files ----
base_cols <- c(
  "clinician_code", "consultant_name", "consultant_hb_name", "link_no",
  "first_episode_upi_number", "first_episode_cis_index", "first_episode_uri",
  "first_episode_dob", "first_episode_sex", "first_episode_admission_date",
  "first_episode_admission_type_description",
  "first_neuro_episode_main_condition", "first_neuro_episode_other_condition_1",
  "first_neuro_episode_other_condition_2", "first_neuro_episode_other_condition_3",
  "first_neuro_episode_other_condition_4", "first_neuro_episode_other_condition_5",
  "nrs_linked_death_date_of_death", "operations_neuro_procedure_occurred"
)
audit_cols <- c(
  "audit_this_neurosurgeon_operated_this_spell", "audit_this_neurosurgeon_last_operation_date",
  "audit_other_neurosurgeon_operation_this_spell_1", "audit_other_neurosurgeon_last_operation_date_1",
  "audit_other_neurosurgeon_operation_this_spell_2", "audit_other_neurosurgeon_last_operation_date_2",
  "audit_date_of_death_correct", "audit_amended_date_of_death",
  "audit_first_neuro_episode_main_condition_correct", "audit_amended_main_condition",
  "audit_first_episode_admission_type_correct", "audit_amended_admission_type",
  "audit_comorbidity_acute_myocardial_infarction", "audit_comorbidity_cerebral_vascular_accident",
  "audit_comorbidity_congestive_heart_failure", "audit_comorbidity_connective_tissue_disorder",
  "audit_comorbidity_dementia", "audit_comorbidity_diabetes", "audit_comorbidity_liver_disease",
  "audit_comorbidity_peptic_ulcer", "audit_comorbidity_peripheral_vascular_disease",
  "audit_comorbidity_pulmonary_disease", "audit_comorbidity_cancer",
  "audit_comorbidity_diabetes_complications", "audit_comorbidity_paraplegia",
  "audit_comorbidity_renal_disease", "audit_comorbidity_metastatic_cancer",
  "audit_comorbidity_severe_liver_disease", "audit_comorbidity_hiv"
)


## Function to select columns during map ----
select_relevant_cols <- function(df){
  operation_date_cols <- names(df)[str_starts(names(df), "operations_operation_date_")]
  cols_to_select <- c(base_cols, audit_cols, operation_date_cols)
  
  df <- df |> select(any_of(unique(cols_to_select)))
}


## Function to convert multiple date formats in a column ----
date_converter <- function(col) {
  converted_dates <- rep(NA_Date_, length(col))
  valid_dates <- !is.na(col)
  
  if (!any(valid_dates)) {
    return(converted_dates)
  }
  
  date_strings_or_numerics <- col[valid_dates]
  numeric_dates <- suppressWarnings(as.numeric(date_strings_or_numerics))
  
  if (any(!is.na(numeric_dates))) {
    excel_dates <- which(!is.na(numeric_dates) & numeric_dates > 0 & numeric_dates < 200000)
    if (length(excel_dates) > 0) {
      converted_dates[valid_dates][excel_dates] <- excel_numeric_to_date(as.integer(numeric_dates[excel_dates]))
    }
  }
  
  needs_string_parse <- which(is.na(converted_dates[valid_dates]))
  if (length(needs_string_parse) > 0) {
    string_dates <- suppressWarnings(
      parse_date_time(date_strings_or_numerics[needs_string_parse], 
                      orders = c("Ymd", "Y-m-d", "d/m/Y", "m/d/Y", "dmY", "mdY", "Y/m/d", "d-m-Y", "m-d-Y"))
    )
    converted_dates[valid_dates][needs_string_parse] <- as.Date(string_dates)
  }
  return(converted_dates)
  
}


## Function to combine trimws and toupper for conciseness
trim_upper <- function(col) {
  col <- toupper(trimws(col))
}


## Function to convert column to yes/no factor ----
factorise_audit_cols <- function(col) {
  col <- factor(trim_upper(col), levels = c("YES", "NO"))
}


## Function to convert column to character and replace values specified with NA ----
character_convert_and_clean <- function(col, values) {
  col <- trim_upper(as.character(col)) 
  col <- if_else(col %in% values, NA_character_, col)
}


## Function to print a "diagnostic report" to the console ----
diagnostic_report <- function(){
  cat("\n\n##### Diagnostic Report for `combined_data` (Post Initial Cleaning) #####\n\n")
  cat("\nDimensions and Structure of combined_data (glimpse):\n"); glimpse(combined_data)
  
  cat("\nNA Counts for key final columns in combined_data (BEFORE regression variable derivations like score, age_group etc.):\n")
  derived_cols_check <- c("final_date_of_death", "final_main_condition", "final_admission_type_description", "operations_neuro_procedure_occurred")
  
  print(sapply(combined_data |> select(any_of(derived_cols_check)), function(x) sum(is.na(x))))
  
  cat("\nFrequency tables for selected categorical columns (from combined_data):\n")
  
  if("final_admission_type_description" %in% names(combined_data)) {
    cat("-- final_admission_type_description (in combined_data) --")
    print(table(combined_data$final_admission_type_description, useNA="ifany"))
  }
  
  if("first_episode_sex" %in% names(combined_data)) {
    cat("\n-- first_episode_sex (in combined_data) --\n")
    print(table(combined_data$first_episode_sex, useNA="ifany"))
  }
  
  cat("\n--- End of `combined_data` Diagnostic Report ---\n")
}


## Function to get comorbidity scores for procedures ----
get_comorbidity_scores <- function(col) {
  current_comorbidity_mapping <- comorbidity_mapping
  names(current_comorbidity_mapping) <- trim_upper(names(current_comorbidity_mapping))
  
  cleaned_codes <- trim_upper(col)
  
  code_4_char <- substr(cleaned_codes, 1, 4)
  code_3_char <- substr(cleaned_codes, 1, 3)
  
  scores_4 <- current_comorbidity_mapping[code_4_char]
  missing_4 <- is.na(scores_4)
  
  scores_3 <- current_comorbidity_mapping[code_3_char]
  
  final_scores <- ifelse(!missing_4, scores_4, scores_3)
  final_scores[is.na(final_scores)] <- 0
  
  return(as.numeric(final_scores))
}


## Function to get comorbidity categories based on comorbidity score ----
get_comorbidity_categories <- function(df) {
  df <- df |> 
    mutate(
      comorbidity_category = case_when(is.na(comorbidity_score) ~ NA_real_,
                                       comorbidity_score <= 0 ~ 1,
                                       comorbidity_score >= 1  & comorbidity_score <= 10 ~ 2,
                                       comorbidity_score >= 11 & comorbidity_score <= 20 ~ 3,
                                       TRUE ~ NA_real_
      )
    )
}


## Function to check if a level exists within a factor column and relevel if so ----
check_and_relevel <- function(df, col, level) {
  if(level %in% levels(df[[col]])) {
    df[[col]] <- relevel(df[[col]], ref = level)
  }
  return(df)
}


# Functions for script '3a funnel plot code' ----
## Function to calculate control limits
control_limits <- function(patients, average_proportion, sd_factor) {
  numerator = 2 * patients * average_proportion / 100 + sd_factor^2
  denominator = patients + sd_factor^2
  adjustment = sd_factor * sqrt(sd_factor^2 + 4 * patients * average_proportion / 100 * (1 - average_proportion / 100))
  
  lower_limit = (numerator - adjustment) / denominator / 2 * 100
  upper_limit = (numerator + adjustment) / denominator / 2 * 100
  
  return(c(lower_limit, upper_limit))
}