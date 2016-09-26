
# ------------------------------------------------------------------------------------------------------------------------ #
# format the data...
require(dplyr)

# ------------------------------------------------ #
#... customers
format_customers <- function(customers_raw){
    customers <- transmute(customers_raw,
                           Reference = as.character(Order.reference), 
                           Earning = grossTotalWithoutDiscounts_eur,
                           #Status = as.factor(CustomerStatus),
                           # Channel = as.factor(channels),
                           City = as.factor(City),
                           CustomerID = as.factor(Customer.reference))
    customers <- customers[complete.cases(customers),]
    return(customers)
}

# ------------------------------------------------ #
#... coordinates
format_coordinates <- function(coordinates_raw){
    # format the hours of the raw data 
    # coordinates_raw <- mutate(coordinates_raw, 
    #                           localPickUpHours = ifelse(nchar(localPickUpHours) < 4,
    #                                                     paste("0", localPickUpHours, sep =""),
    #                                                     localPickUpHours),
    #                           localDropOffHours = ifelse(nchar(localDropOffHours) < 4,
    #                                                      paste("0", localDropOffHours, sep =""),
    #                                                      localDropOffHours),
    #                           localCreatedAtHours = ifelse(nchar(as.character(localCreatedAtHours)) < 2,
    #                                                        paste("0", as.character(localCreatedAtHours), sep =""),
    #                                                        as.character(localCreatedAtHours))
    #                           )
    
    
    # extract and format as a tidy df
  library(stringr)
    coordinates <- transmute(coordinates_raw, 
                             Long = pickUpAddress__geoLocation__coordinates__0, 
                             Lat = pickUpAddress__geoLocation__coordinates__1, 
                             SC = as.factor(str_split_fixed(chosenServiceClass__reference, "-", 3)[, 2]),
                             Date.Order = as.Date(Days.in.createdAt, format = "%m/%d/%y"),
                             Date.PU = as.Date(Days.in.pickUpTimeslot__from, format = "%m/%d/%y"),
                             Date.DO = as.Date(Days.in.dropOffTimeslot__from, format = "%m/%d/%y"),
                             Time.Order = as.POSIXct(paste(Days.in.createdAt, 
                                                           localCreatedAtHours), 
                                                     format = "%m/%d/%y %H"),
                             Time.PU = as.POSIXct(paste(Days.in.pickUpTimeslot__from, 
                                                        localPickUpFromHours), 
                                                  format = "%m/%d/%y %H:%M"),
                             Time.DO = as.POSIXct(paste(Days.in.dropOffTimeslot__from, 
                                                        localDropOffFromHours), 
                                                  format = "%m/%d/%y %H:%M"),
                             Hour.Order = format(as.POSIXct(localCreatedAtHours, format = "%H"), format = "%H:%M"),
                             Hour.PU = format(as.POSIXct(localPickUpFromHours, format = "%H:%M"), format = "%H:%M"),
                             Hour.DO = format(as.POSIXct(localDropOffFromHours, format = "%H:%M"), format = "%H:%M"),
                             Reference = reference)
    coordinates <- coordinates[complete.cases(coordinates),]
    return(coordinates)
}

# ------------------------------------------------ #
#... travelling time
format_travelling_time <- function(travelling_time_raw){
    travelling_time <- transmute(travelling_time_raw,
                                 Day = as.Date(Days.in.forecast__completedAt__date, format = "%m/%d/%y"),
                                 From = destination_from_hours,
                                 To = destination_to_hours,
                                 Cluster = fleet__name, 
                                 From_ID = start__externalId,
                                 To_ID = destination__externalId,
                                 travel_time = forecast__travelingTime,
                                 # idletime = ifelse(is.na(final_idletime),
                                 #                   0, 
                                 #                   final_idletime),
                                 travel_dist = forecast__travelingDistance) %>%
        mutate(interaction_time = ifelse(grepl("-DRI", To_ID),
                                         30, ifelse(grepl("-PU", To_ID),7 , 5)))

    
    travelling_time <- data.table(travelling_time[complete.cases(travelling_time),])
    return(travelling_time)
}