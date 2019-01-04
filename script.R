##############################################
# This is a program for creating transit network using trip based network hierarchy. More details about
# such a network can be found in following reference:
# Khani, A., M. Hickman, and H. Noh. Trip-Based Path Algorithms Using the Transit Network Hierarchy. 
# Networks and Spatial Economics, Vol. 15, No. 3, 2014, pp. 635???653. 
##############################################
# Script created by Pramesh Kumar 12/19/2018
##############################################
# This is the query for the GTFS data. Please note that this is Green Line GTFS data for eastbound direction
library(RPostgreSQL)
drv <- dbDriver("PostgreSQL")
library(multiplex)
location <- "" # Please specify the location where you want to write those files
# Upload your GTFS data to a Postgres server. 
# You can also combine GTFS files seperately. Below is the method to call GTFS data from SQL server
# Connecting to the server 
con <- dbConnect(drv, dbname = "xxxx",
                 host = "xxxx", port = 8100,
                 user = "xxxx", password = "xxxx")
#Querying the serviceID within your time period
serviceId <- dbGetQuery(con, "select service_id from gtfs_feb_mar_2016.calendar where start_date<'2016-03-08' and end_date>'2016-03-08'")
#Filtering for typical weekday service
serviceId <- serviceId[grepl('Weekday-01', serviceId$service_id) == TRUE & grepl('01-', serviceId$service_id) == FALSE, ]

# Querying and combining the gtfs data for different serviceIds (e.g. bus, light rail, sub, etc.)
gtfs <- data.frame()
for (sId in serviceId){
  gtfs <- rbind(gtfs, dbGetQuery(con, paste0("select start_date, end_date, routes.route_id, route_type, route_short_id, route_long_name, trips.trip_id, trips.direction_id, trip_headsign, arrival_time, departure_time, stops.stop_id, stop_name, stop_sequence, stop_desc, stop_lat, stop_lon from gtfs.gtfs_feb_mar_2016.calendar inner join gtfs.gtfs_feb_mar_2016.trips ON (calendar.service_id = trips.service_id and (calendar.service_id = ", "'", sId, "'", ")) join gtfs.gtfs_feb_mar_2016.stop_times ON (stop_times.trip_id = trips.trip_id) join gtfs.gtfs_feb_mar_2016.routes ON (routes.route_id = trips.route_id) join gtfs.gtfs_feb_mar_2016.stops ON (stops.stop_id = stop_times.stop_id)")))
}

#Changing the format of time for ft_input
gtfs$arrival_time <- gsub(":", "", as.character(gtfs$arrival_time))
gtfs$departure_time <- gsub(":", "", as.character(gtfs$departure_time))

# Writing the ft_input_route file
routes <- gtfs[c('route_id', 'route_short_id', 'route_long_name', 'route_type')]
routes <- unique(routes)
write.table(routes, paste0(location, "ft_input_routes.dat"), sep = "\t", row.names = FALSE, quote = FALSE)

# Writing the ft_input_stops.dat
stops <- gtfs[c("stop_id", "stop_name", "stop_desc", "stop_lat", "stop_lon")]
stops <- unique(stops)
stops$capacity <- ""
write.table(stops, paste0(location, "ft_input_stops.dat"), sep = "\t", row.names = FALSE, quote = FALSE)

# Writing the ft_input_stopTimes.dat
stopTimes <- gtfs[c("trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence")]
stopTimes <- unique(stopTimes)
stopTimes <- stopTimes[order(stopTimes$trip_id, stopTimes$stop_sequence), ]
write.table(stopTimes, paste0(location, "ft_input_stopTimes.dat"), sep = "\t", row.names = FALSE, quote = FALSE)

# Writing the ft_input_trips.dat
trips <- gtfs[gtfs$stop_sequence == 1, ]
trips <- trips[c("trip_id", "route_id", "route_type", "departure_time", "direction_id")]
trips$capacity <- ""
trips$shapeId <- ""
trips<- trips[, c("trip_id", "route_id", "route_type", "departure_time", "capacity", "shapeId", "direction_id")]
write.table(trips, paste0(location, "ft_input_trips.dat"), sep = "\t", row.names = FALSE, quote = FALSE)

# Writing the ft_input_zones.dat
location2 <- "Z:\\Projects\\UPassClustering\\Data\\TAZ2010\\"
zones <- read.csv(paste0(location2, "TAZ2010WGSCentroid.csv"))
zones <- zones[c('TAZ', 'Y', 'X')]
colnames(zones) <- c('zoneId',	'Latitude',	'Longitude')
write.table(zones, paste0(location, "ft_input_zones.dat"), sep = "\t", row.names = FALSE, quote = FALSE)


# Writing the ft_input_accessLinks.dat
# This step requires the ft_input_stops.dat and ft_input_zones.dat
start_time <- Sys.time()
library(geosphere)
zones <- read.table(paste0(location, "ft_input_zones.dat"), header = TRUE)
stops <-read.table(paste0(location, "ft_input_stops.dat"), header = TRUE)
access_links <- data.frame()
colnames(access_links) <- c('TAZ',	'stopId', 'dist',	'time')
for (i in seq(from = 1, to = nrow(zones))){
  tmpStops <- stops
  tmpStops$dist <- 0.000621371*distHaversine(c(as.numeric(zones$Longitude[i]), as.numeric(zones$Latitude[i])), stops[c('stop_lon', 'stop_lat')])
  tmpStops <- tmpStops[tmpStops$dist <= 0.75, ]
  if (nrow(tmpStops) != 0){
    tmpStops$time <- tmpStops$dist*60/3
    tmpStops$TAZ <- zones$zoneId[i]
    tmpStops <- tmpStops[c('TAZ',	'stop_id', 'dist',	'time')]
    colnames(tmpStops) <- c('TAZ', 'stopId', 'dist',	'time')
    access_links <- rbind(access_links, tmpStops)
  }
}
write.table(access_links, paste0(location, "ft_input_accessLinks.dat"), sep = "\t", row.names = FALSE, quote = FALSE)
Sys.time() -start_time

# Writing the ft_input_transfers.dat
# This step requires the ft_input_stops.dat
library(geosphere)
stops <-read.table(paste0(location, "ft_input_stops.dat"), header = TRUE)
# Note that the second stop should not be the last stop condtion is not included. This will be taken care by the SBSP
createTransferLinks <- function(i, toStop){
  fromStop <- toStop[i, ]
  toStop$dist <- 0.000621371*distHaversine(c(as.numeric(fromStop$stop_lon), as.numeric(fromStop$stop_lat)), toStop[c('stop_lon', 'stop_lat')])
  toStop <- toStop[toStop$dist <= 0.25, ]
  toStop$fromStop <- fromStop$stop_id
  toStop$time <- toStop$dist*60/3
  toStop <- toStop[toStop$fromStop != toStop$stop_id, ]
  toStop <- toStop[c('fromStop',	'stop_id', 'dist',	'time')]
  toStop <- unique(toStop)
  colnames(toStop) <- c('fromStop', 'toStop', 'dist',	'time')
  toStop <- na.omit(toStop)
  return(toStop)
}

start_time <- Sys.time()
transfers_links <- do.call(rbind,lapply(1:nrow(stops),function(x) createTransferLinks(x, stops)))
Sys.time() -start_time
write.table(transfers_links, paste0(location, "ft_input_transfers.dat"), sep = "\t", row.names = FALSE, quote = FALSE)

# Preparing ft_drivingTime.dat
DrivingTime <- read.csv("S:\\Projects\\Ridesharing Work\\Scripts\\travel_times.csv", header = FALSE)
colnames(DrivingTime) <- c("fromTAZ", "toTAZ", "travelTime")
write.table(DrivingTime, paste0(location, "ft_input_drivingTime.dat"), sep = "\t", row.names = FALSE, quote = FALSE)





# Writing the ft_input_drivingAccessLinks.dat
stops <-read.table(paste0(location, "ft_input_stops.dat"), header = TRUE)
zones <- read.table(paste0(location, "ft_input_zones.dat"), header = TRUE)
driving_access_links <- data.frame()
colnames(driving_access_links) <- c('stopId', 'zoneId', 'dist',	'time')
for (i in seq(from = 1, to = nrow(stops))){
  tmpStops <- zones
  tmpStops$dist <- 0.000621371*distHaversine(c(as.numeric(stops$stop_lon[i]), as.numeric(stops$stop_lat[i])), zones[c('Longitude', 'Latitude')])
  tmpStops <- tmpStops[which.min(tmpStops$dist), ]
  if (nrow(tmpStops) != 0){
    tmpStops$time <- tmpStops$dist*60/40 # 40 miles an hour speed of the car
    tmpStops$stop_id <- stops$stop_id[i]
    tmpStops <- tmpStops[c(	'stop_id', 'zoneId', 'dist',	'time')]
    driving_access_links <- rbind(driving_access_links, tmpStops)
  }
}

write.table(driving_access_links, paste0(location, "ft_input_drivingAccessLinks.dat"), sep = "\t", row.names = FALSE, quote = FALSE)


