

library(rmongodb)
library(plyr)
library(lubridate)
library(data.table)
library(reshape2)

flatlist <- function(mylist){
  lapply(rapply(mylist, enquote, how="unlist"), eval)
}
flattenList <- function(x){
  x<-rbind(lapply(x,function(y){data.table(t(y))}),fill=TRUE)
}



gen_data_demandpred<-function(start_date,last_date){
#loads fields_needed
fields_needed<-list( "start.assignedTo" =1L, 
                     "start.type"=1L,
                     "courier.name"=1L,
                     "fleet.name"=1L,
                     "start.externalId"=1L,
                     "destination.externalId"=1L,
                     "forecast.startAt.date"=1L,
                     "forecast.startAt.tz"=1L,
                     "forecast.completedAt.date"=1L,
                     "forecast.completedAt.tz"=1L,
                     "destination.serviceSlot.from.date"=1L,
                     "destination.serviceSlot.from.tz"=1L,
                     "destination.serviceSlot.to.date"=1L,
                     "destination.serviceSlot.to.tz"=1L,
                     "forecast.travelingDistance"=1L,
                     "forecast.travelingTime"=1L)

#mongo.destroy(mongo)
mongo <- mongo.create(host="172.31.51.215:27017", db="alyx-live")
mongo.is.connected(mongo)
cursor<-mongo.find(mongo, "alyx-live.bi_task_predictions",query=list("start.serviceSlot.from.date" = list("$gt" = start_date,"$lte" = last_date)),  sort = mongo.bson.empty(),fields = fields_needed, options = 4L )
doc3<-mongo.cursor.to.list(cursor)
mongo.destroy(mongo)

#flatten each list
doc3<-lapply(doc3,flatlist)
#remove mongoid fields
doc3<-lapply(doc3,function(x)(x=x[which(sapply(x,class)!="mongo.oid")]))

#combine lists by column names and save as dataframe
doc3<-rbindlist(doc3,use.names=TRUE,fill=TRUE)

doc3<-data.table(doc3)
doc3<-doc3[!duplicated(doc3)]

a<-doc3[,.I[forecast.startAt.tz=="Europe/Berlin"]]
doc3$forecast.startAt.date[a]<-as.character(with_tz(doc3[a,forecast.startAt.date],tzone="Europe/Berlin"))
doc3$destination.serviceSlot.to.date[a]<-as.character(with_tz(doc3[a,destination.serviceSlot.to.date],tzone="Europe/Berlin"))
doc3$destination.serviceSlot.from.date[a]<-as.character(with_tz(doc3[a,destination.serviceSlot.from.date],tzone="Europe/Berlin"))
doc3$forecast.completedAt.date[a]<-as.character(with_tz(doc3[a,forecast.completedAt.date],tzone="Europe/Berlin"))


a<-doc3[,.I[forecast.startAt.tz== "Europe/London"]]
doc3$forecast.startAt.date[a]<-as.character(with_tz(doc3[a,forecast.startAt.date],tzone= "Europe/London"))
doc3$destination.serviceSlot.to.date[a]<-as.character(with_tz(doc3[a,destination.serviceSlot.to.date],tzone= "Europe/London"))
doc3$destination.serviceSlot.from.date[a]<-as.character(with_tz(doc3[a,destination.serviceSlot.from.date],tzone= "Europe/London"))
doc3$forecast.completedAt.date[a]<-as.character(with_tz(doc3[a,forecast.completedAt.date],tzone="Europe/London"))


a<-doc3[,.I[forecast.startAt.tz== "Europe/Paris" ]]
doc3$forecast.startAt.date[a]<-as.character(with_tz(doc3[a,forecast.startAt.date],tzone= "Europe/Paris" ))
doc3$destination.serviceSlot.to.date[a]<-as.character(with_tz(doc3[a,destination.serviceSlot.to.date],tzone= "Europe/Paris" ))
doc3$destination.serviceSlot.from.date[a]<-as.character(with_tz(doc3[a,destination.serviceSlot.from.date],tzone= "Europe/Paris" ))
doc3$forecast.completedAt.date[a]<-as.character(with_tz(doc3[a,forecast.completedAt.date],tzone="Europe/Paris"))
return(doc3)
}

gen_data_idletime<-function(start_date,last_date){
  
doc3<-gen_data_demandpred(start_date,last_date)
nb_driver_raw<-transmute(doc3,heads_id=start.assignedTo,
                         courier__name=courier.name,
                         fleet__name=fleet.name,
                         start__externalId=start.externalId,
                         destination__externalId=destination.externalId,
                         Days.in.forecast__startAt__date=strftime(as.Date(forecast.startAt.date),format="%m/%d/%y"),
                         startat_hours=strftime(forecast.startAt.date,format="%H:%M"),
                         Days.in.forecast__completedAt__date=strftime(as.Date(forecast.completedAt.date),format="%m/%d/%y"),
                         completedat_hours=strftime(forecast.completedAt.date,format="%H:%M"),
                         destination_from_hours=strftime(destination.serviceSlot.from.date,format="%H:%M"),
                         destination_to_hours=strftime(destination.serviceSlot.to.date,format="%H:%M"),
                         forecast__travelingDistance=forecast.travelingDistance,
                         forecast__travelingTime=forecast.travelingTime
                         
)
nb_driver_raw<-data.frame(nb_driver_raw)
return(nb_driver_raw)

}

