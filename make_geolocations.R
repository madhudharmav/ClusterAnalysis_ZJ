flatlist <- function(mylist){
  lapply(rapply(mylist, enquote, how="unlist"), eval)
}
flattenList <- function(x){
  x<-rbindlist(lapply(x,function(y){data.table(t(y))}),use.names=TRUE,fill=TRUE)
}

gen_geolocationdata<-function(start_date,last_date){

mongo <- mongo.create(host="172.31.51.215:27017", db="uk_live")
mongo.is.connected(mongo)

#start_date<-strptime("2016-07-01",format="%Y-%m-%d",tz="UTC")
#last_date <-strptime("2016-07-04",format="%Y-%m-%d",tz="UTC")

fields_needed<-list( "pickUpAddress.geoLocation.coordinates" =1L, 
                     "createdAt"=1L,
                     "reference"=1L,
                     "pickUpTimeslot.from"=1L,
                     "pickUpTimeslot.fromTimeZone"=1L,
                     "dropOffTimeslot.from"=1L,
                     "dropOffTimeslot$fromTimeZone"=1L,
                     "chosenServiceClass.reference"=1L)
                     



cursor<-mongo.find(mongo, "uk_live.intwash_orders",query=list("pickUpTimeslot.from" = list("$gte" = start_date),"dropOffTimeslot.from" = list("$lt" = last_date)),
                   fields = fields_needed,sort = mongo.bson.empty(), options = 4L )


doc<-mongo.cursor.to.list(cursor)
mongo.destroy(mongo)

doc<-lapply(doc,flatlist)
doc<-lapply(doc,function(x)(x=x[which(sapply(x,class)!="mongo.oid")]))
doc<-flattenList(doc) 
#doc<-doc[!which(doc=="NULL",arr.ind = TRUE)[,1]]
#doc<-rbindlist(doc,use.names=TRUE,fill=TRUE)

order_loc<-doc[,pickUpAddress.geoLocation.coordinates]
order_loc<-matrix(unlist(order_loc),nrow=length(order_loc),byrow=T)
colnames(order_loc)<-c("pickUpAddress__geoLocation__coordinates__0","pickUpAddress__geoLocation__coordinates__1")
doc<-data.frame(order_loc,
           Days.in.createdAt=format(do.call("c",doc$createdAt),"%m/%d/%y"),
           reference=unlist(doc$reference),
           Days.in.pickUpTimeslot__from=format(do.call("c",doc$pickUpTimeslot.from),"%m/%d/%y"),
           Days.in.dropOffTimeslot__from=format(do.call("c",doc$dropOffTimeslot.from),"%m/%d/%y"),
           chosenServiceClass__reference=unlist(doc$chosenServiceClass.reference),
           localCreatedAtHours=as.character(do.call("c",doc$createdAt)),
           localPickUpFromHours=as.character(do.call("c",doc$pickUpTimeslot.from)),
           localDropOffFromHours=as.character(do.call("c",doc$dropOffTimeslot.from)),
           X0=unlist(doc$pickUpTimeslot.fromTimeZone)
           ,stringsAsFactors = FALSE
)
doc<-data.table(doc)
a<-doc[,.I[X0=="Europe/Berlin"]]
doc$localCreatedAtHours[a]<-format(doc[a,as.POSIXct(localCreatedAtHours)],"%H:%M",tz="Europe/Berlin")
doc$localPickUpFromHours[a]<-format(with_tz(doc[a,as.POSIXct(localPickUpFromHours)],tzone="Europe/Berlin"),format="%H:%M")
doc$localDropOffFromHours[a]<-format(with_tz(doc[a,as.POSIXct(localDropOffFromHours)],tzone="Europe/Berlin"),format="%H:%M")


a<-doc[,.I[X0=="Europe/London"]]
doc$localCreatedAtHours[a]<-format(doc[a,as.POSIXct(localCreatedAtHours)],"%H:%M",tz="Europe/London")
doc$localPickUpFromHours[a]<-format(with_tz(doc[a,as.POSIXct(localPickUpFromHours)],tzone="Europe/London"),format="%H:%M")
doc$localDropOffFromHours[a]<-format(with_tz(doc[a,as.POSIXct(localDropOffFromHours)],tzone="Europe/London"),format="%H:%M")

a<-doc[,.I[X0=="Europe/Paris"]]
doc$localCreatedAtHours[a]<-format(doc[a,as.POSIXct(localCreatedAtHours)],"%H:%M",tz="Europe/Paris")
doc$localPickUpFromHours[a]<-format(with_tz(doc[a,as.POSIXct(localPickUpFromHours)],tzone="Europe/Paris"),format="%H:%M")
doc$localDropOffFromHours[a]<-format(with_tz(doc[a,as.POSIXct(localDropOffFromHours)],tzone="Europe/Paris"),format="%H:%M")
#doc$X0<-as.numeric(0)


 return(doc)          
}           
