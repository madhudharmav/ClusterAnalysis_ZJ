flatlist <- function(mylist){
  lapply(rapply(mylist, enquote, how="unlist"), eval)
}
flattenList <- function(x){
  x<-rbindlist(lapply(x,function(y){data.table(t(y))}),use.names=TRUE,fill=TRUE)
}
library(plyr)
gen_customerdata<-function(start_date,last_date){
doc3<-data.table()
doc5<-data.table()
mongo <- mongo.create(host="172.31.51.215:27017", db="uk_live")
mongo.is.connected(mongo)

#start_date<-strptime("2016-07-01",format="%Y-%m-%d",tz="UTC")
#last_date <-strptime("2016-07-04",format="%Y-%m-%d",tz="UTC")

fields_needed<-list( "customer" =1L, 
                     "reference" =1L,
                     "state"=1L)
                     



cursor<-mongo.find(mongo, "uk_live.intwash_orders",query=list("pickUpTimeslot.from" = list("$gte" = start_date),"dropOffTimeslot.from" = list("$lt" = last_date)),fields = fields_needed,sort = mongo.bson.empty(), options = 4L )
doc<-mongo.cursor.to.list(cursor)
doc<-lapply(doc,flatlist)


doc<-lapply(doc,function(x){x$customer=mongo.oid.to.string(x$customer);return(x)})
doc<-lapply(doc,function(x)(x=x[which(sapply(x,class)!="mongo.oid")]))
doc<-flattenList(doc) 
doc<-doc[state!="canceled"]


fields_needed<-list( "addresses.createdAt" =1L, 
                     "reference" =1L,
                     "addresses.addressLine2" =1L,
                     "internalData.gender" =1L,
                     "internalData.segmentation" =1L,
                     "internalData.basketGender" =1L,
                     "addresses.label" =1L,
                     "addresses.zip"=1L,
                     "userAccount.email" =1L,
                     "person.name" =1L,
                     "addresses.city" =1L)  


for(i1 in c(1:nrow(doc))){
p<-unlist(doc$customer[i1])
 
cursor<-mongo.find(mongo, "uk_live.intwash_customers",query=list("_id" = mongo.oid.from.string(p)), fields = fields_needed, sort = mongo.bson.empty(), options = 4L )
doc2<-mongo.cursor.to.list(cursor)
doc2<-lapply(doc2,flatlist)
doc2<-lapply(doc2,function(x)(x=x[which(sapply(x,class)!="mongo.oid")]))
cols<-unique(names(unlist(doc2)))
doc2<-unlist(doc2)[cols]
doc2<-data.table(t(doc2))
setnames(doc2,"reference","cus.reference")

doc2<-cbind(doc2,doc[i1,])
doc3<-rbindlist(list(doc3,doc2),use.names=TRUE,fill=TRUE)
}
doc3$customer<-NULL

fields_needed<-list( "grossTotalWithoutDiscounts" =1L, 
                     "discounts.voucherCode" =1L)  

for(i1 in c(1:nrow(doc3))){
p<- unlist(doc3$reference[i1])

cursor<-mongo.find(mongo, "uk_live.intwash_invoices",query=list("invoiceNumber" = p), fields = fields_needed, sort = mongo.bson.empty(), options = 4L )
doc4<-mongo.cursor.to.list(cursor)
doc4<-lapply(doc4,flatlist)
doc4<-lapply(doc4,function(x)(x=x[which(sapply(x,class)!="mongo.oid")]))
if(length(doc4)==0){doc4<-data.table(grossTotalWithoutDiscounts=NA)
}else{doc4<-data.table(t(unlist(doc4)))}
doc4<-cbind(doc4,doc3[i1,])
doc5<-rbindlist(list(doc5,doc4),use.names=TRUE,fill=TRUE)

}
doc5<-doc5[!is.na(grossTotalWithoutDiscounts)]
mongo.destroy(mongo)



doc5<-data.frame(Date=format(as.Date(as.POSIXct(as.numeric(doc5$addresses.createdAt),origin='1970-01-01')),format="%m/%d/%y"),
                Customer.reference=unlist(doc5$cus.reference),
                Order.reference=unlist(doc5$reference),
                Voucher.code=unlist(doc5$discounts.voucherCode),
                State=unlist(doc5$state),
                Zip=unlist(doc5$addresses.zip),
                User.Email=unlist(doc5$userAccount.email),
                User.Name=unlist(doc5$person.name),
                City=unlist(doc5$addresses.city),
                grossTotalWithoutDiscounts_eur=unlist(doc5$grossTotalWithoutDiscounts),
                Label=unlist(doc5$addresses.label),
                pickUpAddress__addressLine2=unlist(doc5$addresses.addressLine2),
                internalData__gender=unlist(doc5$internalData.gender),
                internalData__segmentation=unlist(doc5$internalData.segmentation),
                internalData__segmentationGender=unlist(doc5$internalData.basketGender),
                stringsAsFactors = FALSE)
doc5$grossTotalWithoutDiscounts_eur<-as.numeric(doc5$grossTotalWithoutDiscounts_eur)
                

return(doc5)
}
