#install.packages('sparklyr')
#install.packages('dplyr')
library(sparklyr)
library(dplyr)

# ------------------------------ Change Col Names ------------------------------
changeNames <- function(df){
  names(df) <- c("Borough", "Hour", "Date", "Cars_And_Taxis", "Motorbikes", 
                 "Buses_and_Coaches", "LGVs","HGVs","All_Vehicles")
  return(df)
}
# ------------------------------------------------------------------------------

# ----------------------------- Reduce dataframe -------------------------------
filterData <- function(df){
  # Filter by year (2018-2021), region_name
  filtered <- df %>%
    filter(year(count_date) %in% c(2018:2021) & region_name == "London")
  return(filtered)
}
# ------------------------------------------------------------------------------

# ------------------------- TODO//: Add counts for both directions -------------

addCounts <- function(df){
  # Counts are measured in both directions, thus add them to get total traffic
  # that passes through area 

  # aggregate count_values_1, count_values_2, and count_values_3 by local_authority_name, hour, and count_date
  agg_df <- aggregate(cbind(cars_and_taxis, two_wheeled_motor_vehicles, 
                            buses_and_coaches, lgvs, all_hgvs, 
                            all_motor_vehicles) ~ local_authority_name + hour 
                      + count_date, data = df, sum)
  #write.csv(agg_df, "addedData.csv", row.names = FALSE)
  
  # view the aggregated dataframe
  #print(agg_df)
  return(agg_df)
}
# ------------------------------------------------------------------------------

# -------------------------- Remove Outliers -----------------------------------
remOutliers <- function(df){
  # Remove values that exceed 3*Standard deviation 
  threshold <-3
  # Calculate Standard deviation and mean for each borough
  sd_Mean_List <- df %>% 
    group_by(Borough) %>% 
    summarize(sd = sd(All_Vehicles), mean = mean(All_Vehicles))
  # For each borough remove if they exceed mean - 3*sd or mean + 3*sd
  df_filtered <- df %>% 
    left_join(sd_Mean_List, by = "Borough") %>% 
    filter(All_Vehicles > (mean - threshold * sd) & 
             All_Vehicles < (mean + threshold * sd)) %>% 
    select(-c(mean, sd))
  #print(df_filtered)
  return(df_filtered)
  
}
# ------------------------------------------------------------------------------

# --------------------- Check/Remove missing Values ----------------------------
checkMissing <- function(df) {
  # Collect data frame in R to calculate missing values
  missing_local <- collect(df)
  # Count missing values in each column
  missing_counts <- sapply(missing_local, function(x) sum(is.na(x)))
  # Get the column names that have no missing values
  cols_to_keep <- names(missing_counts[missing_counts == 0])
  # Remove columns that contain missing values
  df <- df %>% select(cols_to_keep)
  return(df)
}
# --------------------- Change format of time ----------------------------------
formatTime <- function(df){
  # Replace 1,2,3 with 01:00:00,02:00:00,03:00:00 etc...
  df <- df %>%
    mutate(hour = ifelse(hour < 10, paste0("0", hour, ":00:00"), 
                         paste0(hour, ":00:00")))
  return(df)
}
# ------------------------------------------------------------------------------

main <- function(){
  # Connect to Apache Spark
  sc <- spark_connect(master = "local")
  # Read vehicle count csv file into Spark data frame 
  df <- spark_read_csv(sc, "vehicleCount.csv", infer_schema = TRUE)
  # Pipeline for transforming data
  df <- df %>%
    # Filter by loc and date
    filterData() %>%
    # Remove missing values
    checkMissing() %>%
    # Format the time
    formatTime() %>%
    # Combine counts for both directions
    addCounts() %>%
    # Change column names such that they are consistent between datasets
    changeNames() %>%
    # Remove outliers
    remOutliers() 
  write.csv(df, "processedDataVehicle.csv", row.names = FALSE)
  #print(df)  
  # Disconnect from Spark connection
  spark_disconnect(sc)
}

main()

