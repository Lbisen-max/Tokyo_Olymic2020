
# Tokyo Olympics Data Analytics | End-To-End Data Engineering Project

This project provides a data engineering and anlytical journey on the Tokyo Olympic dataset. Starting with a CSV on kaggle, the data is ingested into the local system. then transformed in local system also. additionaly the enriched transformed data, once again housed in MYSQL database. further it is used for Analytics using PowerBI.




## Dataset Used
This includes information on over 11,000 competitors in 47 sports and 743 teams competing in the Tokyo Olympics in 2020 and 2021. This dataset includes information on the competing teams, athletes, coaches, and entries broken down by gender. It lists their names, the nations they represent, the discipline, the contestants' gender, and the coaches' names.

Source(Kaggle): 2021 Olympics in Tokyo
## Services Used
1. PyCharm IDE
2. Python (Version 3.7.8)
3. Spark (Version 2.3.2)
4. MySql 
5. PowerBI 


##
## Workflow

### Initial Setup

1. Downlaoded the required tool like Python , Pycharm , PwerBI , 
   Spark and MySQL workbanch.
2. Opted data from Kaggle data source and stored in local system.

### Folder Structure 
Tokyo_Olymic2020
├── src
│   ├── main/
│   │    ├──ProjectConfig
│   │    └── Config
│   │    │      ├── aws_delete.py
│   │    │      ├── database_delete.py
│   │    │      └── local_file_delete.py
│   │    └── download/
│   │    │      └── aws_file_download.py
│   │    └── move/
│   │    │      └── move_files.py
│   │    └── read/
│   │    │      ├── aws_read.py
│   │    │      └── database_read.py
│   │    └── transformations/
### Data Ingestion and Data Transformation using Pyspark

