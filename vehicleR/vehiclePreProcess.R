library(sparklyr)
library(dplyr)

# --------------------- Reduce dataframe --------------------------------------
filterData <- function(df){
  # Filter by year (2018-2021), region_name
  filtered <- df %>%
    filter(year(count_date) %in% c(2018:2021) & region_name == "London")
  return(filtered)
}

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
    formatTime()
  print(df)  
  # Disconnect from Spark connection
  spark_disconnect(sc)
}

main()
