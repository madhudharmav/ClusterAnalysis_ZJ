require(geosphere) # calculate the distances on earth
require(maptools) 
require(ggmap) # ggle map background
require(RColorBrewer) 
require(viridis) # colors 
require(dplyr) # df manipulation
#library(ks) # for 2D kernel
require(ggplot2) # for graphic device
# require(highcharter) # for fancy graphs
require(leaflet) # interactive maps
require(PBSmapping)
require(sp)
require(stringr)

# ------------------------------------------------------------------------------------------------------------------------ #
# Load the data
#setwd("C:/Users/Madhu/Documents/Availability")
# if(!exists("path_data")) {path_data <- paste(getwd(), 
#                                              "/CW 31", 
#                                              sep = "")}
# coordinates_raw <- read.csv(paste(path_data, "Geolocation.csv", 
#                                   sep = "/"), stringsAsFactors = FALSE, header = TRUE) # order dashboard
# customers_raw <- read.csv(paste(path_data, "Customersbydate.csv", 
#                                 sep = "/"), stringsAsFactors = TRUE, header = TRUE, row.names = NULL) # customer dashboard
# travelling_time_raw <- read.csv(paste(path_data, "Idletimepivot.csv", 
#                                       sep = "/"), stringsAsFactors = FALSE, header = TRUE) # from idle time dashboard
# 

coordinates_raw <- gen_geolocationdata(start_date,last_date)
customers_raw <- gen_customerdata(start_date,last_date)
travelling_time_raw <-gen_data_idletime(start_date,last_date)

# ------------------------------------------------------------------------------------------------------------------------ #
# format the data...
source("format_functions.R")
customers <- format_customers(customers_raw)
coordinates <- format_coordinates(coordinates_raw)
travelling_time <- format_travelling_time(travelling_time_raw)

# ------------------------------------------------------------------------------------------------------------------------ #
# join - filter the data

travelling_time <- travelling_time[which(travelling_time$Day >= days[1] &
                                             travelling_time$Day <= days[2]), ]

df_all <- inner_join(customers, coordinates, by = c("Reference" = "Reference"))

df_all <- rbind(mutate(df_all, Reference_full = paste(Reference, "-DO", sep = ""), 
                       Type = "DO"),
                mutate(df_all, Reference_full = paste(Reference, "-PU", sep = ""), 
                       Type = "PU")) 


# add the travelling times
#... to
df_all <- inner_join(x = df_all, 
                     y = dplyr::select(travelling_time, To_ID, travel_time, interaction_time, 
                                                   From, Day, Cluster, travel_dist), 
                by = c("Reference_full" = "To_ID")) %>% 
    dplyr::rename(to_time = travel_time)

#... from
df_all <- inner_join(x = df_all, 
                     y = dplyr::select(travelling_time, From_ID, travel_time), 
                by = c("Reference_full" = "From_ID")) %>%
    dplyr::rename(from_time = travel_time)

# add travelling time
only_hours <- function(x){
    x <- as.character(x)
    x <- as.numeric(unlist(strsplit(x, ":")))[seq(1, 2*length(x), 2)]
    return(paste(as.character(x), ":00", sep = ""))
}

df_all <- mutate(df_all, 
                 travel_time = to_time + from_time, 
                 Type = as.factor(Type),
                 Weekday = weekdays(as.Date(Day, format = "%Y-%m-%d")), 
                 From = only_hours(From))



