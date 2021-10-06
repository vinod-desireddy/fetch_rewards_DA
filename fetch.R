#Fetch rewards coding exercise

#Loading the required libraries
library(jsonlite)
library(plyr)
library(dplyr)
library(tidyr)
library(janitor)
library(stringr)

#setting the working directory
#setwd("~/Desktop/fetch")

#reading the new line delimited unstructured json file using 'jsonlite' package
receipts <- stream_in(file('receipts.json'))
typeof(receipts) #this is list now.

#changing list to a dataframe
receipts <- as.data.frame(receipts)
typeof(receipts)
head(receipts)
names(receipts)
str(receipts[,1]) 
str(receipts[,"rewardsReceiptItemList"])
# if we view this dataset this is in a nested list format. where the column _id is a dataframe of 1 variable and and the column "rewardsReceiptItemList" is dataframe of many variables.

# since the column id is a dataframe of 1 variable, we can just convert it as a normal
#extracting the id column as a separate data frame
a = as.data.frame(list(receipts[,1]))
#replacing the nested dataframe in the dataset receipts as a normal column
receipts[,1] <- a
#deleting the temporary variable 'a'
rm(a)
names(receipts)
#cleaning the column names
receipts <- clean_names(receipts)
names(receipts)

# since the column rewardsReceiptItemList is a nested dataframe of multiple variable, we can extract it and make it as a separate dataframe by itself.

#extracting columns id and rewards_receipt_item_list columns
rewardsReceiptItemList <- receipts[,c("id","rewards_receipt_item_list")]

#since id column is the primary, and the other column is nested, we will extract all the nested observations and make them as normal observations using the for loop.
#initialising an empty dataframe
abc = data.frame()
for(i in 1: nrow(rewardsReceiptItemList)){
  if(rewardsReceiptItemList[i,2] != 'NULL'){
    for(j in 1:nrow(as.data.frame(rewardsReceiptItemList[i,2]))){
      abc = bind_rows(abc,cbind(rewardsReceiptItemList[i,1],as.data.frame(rewardsReceiptItemList[i,2])[j,]))
    }
  }
}

#removing the temporary variables created as part of for loop
rm(i)
rm(j)

#converting the new dataset as a dataframe
rewardsReceiptItemList = as.data.frame(abc)
rm(abc)

# we can remove this nested column the original dataset because we have extracted it as a separate data frame
receipts <- receipts[-which(names(receipts)=="rewards_receipt_item_list")]
receipts <- clean_names(receipts)
rewardsReceiptItemList <- clean_names(rewardsReceiptItemList)
colnames(rewardsReceiptItemList)[1] <- colnames(receipts)[1]

#Now we have two dataframes. One is receipts, which contains receipt details for every receipt id.
# the other dataset is rewardreceiptitemlist, which contains item list of each receipt as each receipt can have multiple items. Also column ID is not longer the primary key in this dataset.

#checking for duplicate rows in both datasets
nrow(unique(receipts)) == nrow(receipts)
nrow(unique(rewardsReceiptItemList)) == nrow(rewardsReceiptItemList)
#here we can see that both the datasets do not have any duplicate rows.
#checking for null values
apply(receipts,2,function(x) sum(is.na(x)))
#we can see that the 'reward' related columns are 50% empty approximately
apply(rewardsReceiptItemList,2,function(x) sum(is.na(x)))
#we can observe that most of the columns in this dataset are 90% null values 

#we can also observe that there are some special characters in some columns in the receipts dataset. SQL doesnot support these special characters. We will remove these special characters in these columns so that we can load them in to sql database.
rewardsReceiptItemList$rewards_group <- str_replace_all(rewardsReceiptItemList$rewards_group,'®', '_')
rewardsReceiptItemList$original_receipt_item_text <- str_replace_all(rewardsReceiptItemList$original_receipt_item_text,'€', '')
rewardsReceiptItemList$description <- str_replace_all(rewardsReceiptItemList$description,'€', '')

# Brands dataset
# reading the json file
brands <- stream_in(file('brands.json'))
brands <- as.data.frame(brands)
str(brands)
# we can see that there are two nested columns in this dataset. they are id  and cpg

# 'id' is a dataframe with 1 variable, and cpg is a dataframe with 2 variables.

# we can just extract id column and make it as a normal column
a = as.data.frame(list(brands[,1]))
brands[,1] = a

# column cpg is a dataframe of 2 variables, we can make them as two columns
a = as.data.frame(list(brands[,5][1]))
b = as.data.frame(list(brands[,5][2]))
brands[,5] = a
brands = cbind(brands[,1:5],b,brands[,6:8])
a = as.data.frame(list(brands[,5]))
brands[,5] = a
b = as.data.frame(list(brands[,6]))
brands[,6] = b
#cleaning the column names of the dataset
brands <- clean_names(brands)
# removing the temporary variables
rm(a)
rm(b)

#checking for the duplicate rows in the brands dataset
nrow(unique(brands)) == nrow(brands)
# we can observe that there are no duplicate rows in this dataset

# checking for null values in the dataset
apply(brands,2,function(x) sum(is.na(x)))
# we can observe that columns category_code,top_brand,brand_code have more than 50% of the null values

#we can also observe that there are some special characters in some columns in the brands dataset. SQL doesnot support these special characters. We will remove these special characters in these columns so that we can load them in to sql database.

#checking for special characters
brands %>% select(brand_code) %>% 
  filter(str_detect(brand_code,'[:alnum:,T]'),str_detect(brand_code,"[@'!]",T))

brands %>% select(name) %>% 
  filter(str_detect(name,'[:alnum:,T]'),str_detect(name,"[@'!]",T))

#replacing the special characters with empty space
brands$name <- str_replace_all(brands$name, '[®™€’]', '')
brands$brand_code <- str_replace_all(brands$brand_code, '[®™€’]', '')

# reading the json file of the users dataset
users <- stream_in(file('users.json'))
typeof(users)
str(users)
#we can see that only the id column is the nested dataframe of 1 variable. We can just extract it and make it as a normal column since it has only 1 variable
a = as.data.frame(list(users[,1]))
users[,1] = a
# removing the temporary variable
rm(a)
#cleaning the column names of the users
users <- clean_names(users)

#checking for the duplicate rows in the dataset
nrow(unique(users)) == nrow(users)
#we can see that there are some duplicate rows in this dataset
(nrow(users)-nrow(unique(users)))*100/nrow(users)
#we can see that there are 57% approx rows are duplicate

#removing the duplicate rows
users <- unique(users)
#before removing the duplicates, there 495 observations and there are 212 observations after removing the duplicates

#Now we have 4 structured datasets 'receipts', rewardsReceiptItemList, brands and users from the 3 unstrucured json files recipts,brands and users.

#we can export these structured datasets as csv files using the following commands
#write.csv(receipts,file = "receipts.csv",row.names = F)
#write.csv(rewardsReceiptItemList,file = "rewardsReceiptItemList.csv",row.names = F)
#write.csv(brands,file = "brands.csv",row.names = F)
#write.csv(users,file = "users.csv",row.names = F)


#stuffDB <- dbConnect(MariaDB(), user = "root", password = "3327", dbname = "fetchdb", host = "localhost")


#In order to push structured datasets in to the SQL data base, we can use the "RMySQL" package and connect R to mysql. 

#install.packages("RMySQL")
#loading the required library
library("RMySQL")
#opening fetch (an empty database that i have created using mysql workbench) database connection from R
connection <- dbConnect(MySQL(), user = 'root', password = '3327', host = 'localhost', dbname = 'fetchdb')
#listing the tables in the fetch database
dbListTables(connection)
#we can see that there are not tables in this datase yet, as we have not pushed the datasets yet.

#initialising the query connection
dbSendQuery(connection, "SET GLOBAL local_infile = true;")

#writing the tables in to the sql
#dbWriteTable(connection, "users", users, overwrite=TRUE)

#dbWriteTable(connection, "brands", brands, overwrite=TRUE)

#dbWriteTable(connection, "receipts", receipts, overwrite=TRUE)

#dbWriteTable(connection, "rewardsReceiptItemList", rewardsReceiptItemList, overwrite=TRUE)

# now that we have pushed all 4 structured datasets in to sql database, we can query it to find the insights.