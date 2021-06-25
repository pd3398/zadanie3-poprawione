library(DBI)
library(dplyr)
library(RSQLite)

konta <- read.csv("konta.csv")
View(konta)



# zadanie 1

rankAccount <- function(dataFrame,colName,groupName,valueSort,num){
  tmp <- select(konta, colName, valueSort)
  select_tmp <- filter(tmp, tmp[[colName]] == groupName)
  array_of_select_column <- array(select_tmp[[valueSort]])
  order_array <- sort(array_of_select_column,decreasing = TRUE)
  
  finally_data <- order_array[1:num]
  finally_data <- as.data.frame(finally_data)
  colnames(finally_data) <- c("saldo")
  return(finally_data)
  
}

rankAccount(konta, "occupation", "NAUCZYCIEL", "saldo", 10)



# zadanie 2

rankAccountBigDatatoChunk <- function(filename, size, colName, groupName, valueSort, num){
  fileConnection <- file(description = filename, open = "r")
  data <- read.table(fileConnection,nrows = size, header=TRUE, fill = TRUE, sep = ",")
  columnsNames <- names(data)
  output <- NULL
  repeat{
    
    if (nrow(data) == 0){
      close(fileConnection)
      break
    }

    data <- na.omit(data)
    data <- read.table(fileConnection, nrows = size, col.names = columnsNames, fill = TRUE, sep = ",")
    new_data <- data %>%select(colName, valueSort) %>%filter(data[[colName]] == groupName)
    output <- rbind(output,new_data)
  }
  output%>%arrange(desc(!!sym(valueSort)))%>%select(saldo)%>%head(num)
}

rankAccountBigDatatoChunk("konta.csv", 1000, "occupation", "NAUCZYCIEL", "saldo", 10)



# zadanie 3

tabelaZbazyDanych<-function(filepath,dbpath,tablename,size,sep=",",header=TRUE,delete=TRUE){
  ap<-!delete
  ov<-delete
  fileConnection<- file(description = filepath,open="r")
  dbConn<-dbConnect(SQLite(),dbpath)
  data<-read.table(fileConnection,nrows=size,header = header,fill=TRUE,sep=sep)
  columnsNames<-names(data)
  dbWriteTable(conn = dbConn,name=tablename,data,append=ap,overwrite=ov)
  repeat{
    if(nrow(data)==0){
      close(fileConnection)
      dbDisconnect(dbConn)
      break
    }
    data<-read.table(fileConnection,nrows=size,col.names=columnsNames,fill=TRUE,sep=sep)
    dbWriteTable(conn = dbConn,name=tablename,data,append=TRUE,overwrite=FALSE)
  }
}


tabelaZbazyDanych("konta.csv", "konta.sqlite", "konta", 1000)

dbp <- "konta.sqlite"
con <- dbConnect(SQLite(),dbp)

dbGetQuery(con, "SELECT saldo FROM konta WHERE occupation = 'NAUCZYCIEL' ORDER BY saldo DESC LIMIT 10;")

dbDisconnect(con)


