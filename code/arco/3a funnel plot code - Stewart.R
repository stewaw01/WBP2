library(plotly)
library(hablar)

# Source functions script
source(paste(here::here(), "code", "arco", "arco_functions.R", sep = "/"))

# Flag for pre-audit or audited data ----
audited = FALSE

if (audited == TRUE){
  model_input_data <- model_input_data_audited
} else {
  model_input_data <- model_input_data_preaudit
}

# Create dataframe for crude rate funnel plot ----
data_crude_rate <- model_input_data |>
  select(link_no, consultant_name, death_within_30_days_of_procedure) |>
  group_by(consultant_name, link_no) |>
  slice(n()) |>
  ungroup() |>
  mutate(patient_count = 1) |>
  group_by(consultant_name) |>
  summarise(patients = sum(patient_count, na.rm = T), deaths = sum(death_within_30_days_of_procedure, na.rm = T)) |>
  ungroup() |>
  mutate(crude_mortality_rate = deaths/patients * 100)


## Calculate crude rate proportion of deaths
average_proportion <- sum(data_crude_rate$deaths) / sum(data_crude_rate$patients) * 100


## Add columns with the upper and lower limts
data_crude_rate <- data_crude_rate |>
  rowwise() |>
  mutate(
    crude_lower_2sd = control_limits(patients, average_proportion, 1.96)[1],
    crude_upper_2sd = control_limits(patients, average_proportion, 1.96)[2],
    crude_lower_3sd = control_limits(patients, average_proportion, 3.0902)[1],
    crude_upper_3sd = control_limits(patients, average_proportion, 3.0902)[2]
  ) |>
  ungroup()


# Create plotly funnel plot for crude mortality rate ----
plotly_crude_rate <- plot_ly(data_crude_rate) |>
  add_markers(
    x = ~patients, 
    y = ~crude_mortality_rate, 
    marker = list(color = "orange"), 
    name = "Mortality Rate",
    text = ~consultant_name,  # Add consultant names as hover text
    hoverinfo = "text+x+y"    # Display consultant name, x value, and y value on hover
  ) |>
  add_lines(x = ~patients, y = ~crude_lower_2sd, line = list(color = "lightblue"), name = "Lower Limit (2 SD)") |>
  add_lines(x = ~patients, y = ~crude_upper_2sd, line = list(color = "lightcoral"), name = "Upper Limit (2 SD)") |>
  add_lines(x = ~patients, y = ~crude_lower_3sd, line = list(color = "darkblue"), name = "Lower Limit (3 SD)") |>
  add_lines(x = ~patients, y = ~crude_upper_3sd, line = list(color = "darkred"), name = "Upper Limit (3 SD)") |>
  layout(
    title = "Crude Mortality Rate Funnel Plot",
    xaxis = list(title = "Number of Patients"),
    yaxis = list(title = "Crude Mortality Rate (%)", range = c(-1, 16)),
    legend = list(
      title = list(text = "Legend"),
      x = 0.5,  # Adjust the x position for legend (centered at the bottom)
      y = -0.15, # Adjust the y position for legend (below the plot)
      orientation = "h"  # Display legend horizontally
    )
  )

plotly_crude_rate


# Create a Linear Model ----
logistic_new <- glm(death_within_30_days_of_procedure ~ age_group + sex + type_of_admission_first_episode + diagnosis_grouping + comorbidity_category,
                    data = model_input_data,
                    family = binomial)


# Use model to predict deaths for each consultant ----
data_standardised_rate_pred <- as.data.frame(predict(logistic_new, model_input_data, type = "response"))
colnames(data_standardised_rate_pred) <- c("pred_death30")

data_standardised_rate <- bind_cols(model_input_data, data_standardised_rate_pred)

data_surgeon_predicted_deaths <- data_standardised_rate |>
  group_by(consultant_name) |>
  summarise(predicted_deaths = sum_(pred_death30), actual_deaths = sum_(as.numeric(death_within_30_days_of_procedure))) |>
  ungroup()


## Add columns with the upper and lower limts
data_surgeon_predicted_deaths <- data_surgeon_predicted_deaths |>  
  mutate(isr = (actual_deaths / predicted_deaths) * 100 ) |>
  mutate(o_lower_2sd = (predicted_deaths) * (1 - (1/(9 * (predicted_deaths))) - (1.96   / (3*sqrt(predicted_deaths))))^3) |>
  mutate(o_upper_2sd = (predicted_deaths+ 1) * (1 - (1/(9 * (predicted_deaths + 1))) + (1.96   / (3*sqrt(predicted_deaths+1))))^3) |>
  mutate(o_lower_3sd = (predicted_deaths) * (1 - (1/(9 * (predicted_deaths))) - (3.0902 / (3*sqrt(predicted_deaths))))^3) |>
  mutate(o_upper_3sd = (predicted_deaths+ 1) * (1 - (1/(9 * (predicted_deaths + 1))) + (3.0902 / (3*sqrt(predicted_deaths+1))))^3) |>
  mutate(isr_lower_2sd = (o_lower_2sd/predicted_deaths)*100) |>
  mutate(isr_upper_2sd = (o_upper_2sd/predicted_deaths)*100) |>
  mutate(isr_lower_3sd = (o_lower_3sd/predicted_deaths)*100) |>
  mutate(isr_upper_3sd = (o_upper_3sd/predicted_deaths)*100)


# Create plotly funnel plot for Indirectly Standardised Ratio ----
plotly_ISR <- plot_ly(data_surgeon_predicted_deaths) |>
  add_markers(
    x = ~predicted_deaths, 
    y = ~isr, 
    marker = list(color = "orange"), 
    name = "Indirectly Standardized Ratio (ISR)",
    text = ~consultant_name,  # Add consultant names as hover text
    hoverinfo = "text+x+y"    # Display consultant name, x value, and y value on hover
  ) |>
  add_lines(x = ~predicted_deaths, y = ~isr_lower_2sd, line = list(color = "lightblue"), name = "ISR Lower Limit (2 SD)") |>
  add_lines(x = ~predicted_deaths, y = ~isr_upper_2sd, line = list(color = "lightcoral"), name = "ISR Upper Limit (2 SD)") |>
  add_lines(x = ~predicted_deaths, y = ~isr_lower_3sd, line = list(color = "darkblue"), name = "ISR Lower Limit (3 SD)") |>
  add_lines(x = ~predicted_deaths, y = ~isr_upper_3sd, line = list(color = "darkred"), name = "ISR Upper Limit (3 SD)") |>
  layout(
    title = "Indirectly Standardised Ratio (ISR) Funnel Plot",
    xaxis = list(title = "Predicted Deaths"),
    yaxis = list(title = "ISR (%)", range = c(-1, 800)),
    legend = list(
      title = list(text = "Legend"),
      x = 0.5,  # Adjust the x position for legend (centered at the bottom)
      y = -0.15, # Adjust the y position for legend (below the plot)
      orientation = "h"  # Display legend horizontally
    )
  )

plotly_ISR
