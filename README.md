# Tokyo Olympics Data Analytics | End-To-End Data Engineering Project

This project provides a data engineering and anlytical journey on the Tokyo Olympic dataset. Starting with a CSV on kaggle, the data is ingested into the local system. then transformed in local system also. additionaly the enriched transformed data, once again housed in MYSQL database. further it is used for Analytics using PowerBI.

# Dataset Used

This includes information on over 11,000 competitors in 47 sports and 743 teams competing in the Tokyo Olympics in 2020 and 2021. This dataset includes information on the competing teams, athletes, coaches, and entries broken down by gender. It lists their names, the nations they represent, the discipline, the contestants' gender, and the coaches' names.

Source(Kaggle): 2021 Olympics in Tokyo

# Inventory of Resources
PyCharm IDE
Python (Version 3.7.8)
Spark (Version 2.3.2)
MySql
PowerBI

# Phases
### 1. Data Acquisition
Acquire the data listed in the project resources. This initial collection included data loading into local system.

List the dataset(s) acquired :

* medals_total.csv - dataset contains all medals grouped by country as here.
* medals.csv - dataset includes general information on all athletes who won a medal.
* athletes.csv - dataset includes some personal information of all athletes.
* coaches.csv - dataset includes some personal information of all coaches.
* technical_officials - dataset includes some personal information of all technical officials.
  
### 2. Data Understanding
Structure Investigation : Checked general shape of the datasets, as well as the data types of features.
Quality Investigation : The main goal to have a global view on the datasets with regards to things like duplicate values,missing values and unwanted entries or recording errors. here I found "athletes.csv" and "coaches.csv" many missing values.additionaly, some of the column like "URL" does not look more authentic hence that can be removed.
### 3. Data Preparation
After structure and quality Investigation it has observed that columns like 'birth_place','birth_country','residence_place','residence_country','url','height_m/ft' does not look generalize hence this has been removed.

### Schema Overview

![image](https://github.com/Lbisen-max/Tokyo_Olymic2020/assets/79071673/32350306-8f29-4286-ba09-0034ae903ea9)


### Folder Structure 

![image](https://github.com/Lbisen-max/Tokyo_Olymic2020/assets/79071673/190f8912-2e0f-46a4-8b79-9ae127fa8a66)



### Data Ingestion and Data Transformation using Pyspark (ETL)
Imported spark on DataIngestionAndTransformation.py file and performed required transformed functions also. Created a config.ini file under projectconfig folder
which contains required ingestion path and schema of the tables.

After transformation data rewritten into local and MYSQL server for further analysis. additionaly , MYSQL serer connected with the PowerBI for visual data understanding.

![image](https://github.com/Lbisen-max/Tokyo_Olymic2020/assets/79071673/a8f52cfd-f3f5-4e99-affa-a88f6f549aec)
![image](https://github.com/Lbisen-max/Tokyo_Olymic2020/assets/79071673/4ac166d4-3355-43ea-807b-2f6c14ae677d)
![image](https://github.com/Lbisen-max/Tokyo_Olymic2020/assets/79071673/7bb3b00f-3a10-47b6-b10d-d7b4a359d7ce)
![image](https://github.com/Lbisen-max/Tokyo_Olymic2020/assets/79071673/7e1ce3dc-9892-440e-9e01-b5b29c0e0b8b)






