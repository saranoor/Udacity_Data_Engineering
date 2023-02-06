### Project:Data Warehouse

#### Introduction
The primary goal of the project is to create a database and build an ETL pipeline for a startup name
Sparkify. Sparkify has seen an increase in their users and songs and are interested to move there database
to cloud. For this purpose, an ETL pipeline is built that takes raw data of songs and users from S3 bucket, apply 
certain transformation to the data, and loads into dimensional tables. This way, the analytics team would
be able to research data and find valuable insights for Sparkify growth. 

#### Database Schema
- The structure of database is star schema. With fact table songplays sit at the center and other dimensional tables songs, users,
artists, time branch off to form the points in star. The choice of start schema over other is base upon that our main information
is about song and the activity of the user. So songplays table store the unique record and dimension table contains the supporting 
information i.e information about artist, users, time, and songs.
- Also since the database needs to used for analytical purposes by Sparkify team, star schema is a reasonable choice as it supports
online analytical processing (OLAP) and is optimized for querying large data sets. In star schema it is easier to aggregate information.

![alt text](schema_design.JPG?raw=true)

#### ETL Design 
ETL pipeline created can be visualised using the figure shown. First we connect to S3 bucket and copy the files related to staging events and songs data
into Redshift usign SQL queries written with Python wrapper. Next, usign the loaded data we insert information into our start based schema table.

![alt text](etl_design.png?raw=true)

#### Setup 

There are 4 main files to create the database and design an ETL pipeline
- dwh.cfg - it is a configuration file that consists of variables for cluster, IAM role and file names
- create_tables.py is a script that either creates or drop tables 
- etl.py is a python script that copy data from S3 to staging table and insert from stagign to datawarehouse
- sql_queries is the main file that consists of all the necessary queries needed to create or drop table/copy and insert in tables

To run the ETL pipeline:
1. Ensure that a redshift cluster exists and an IAM role is associated with that cluster.
2. Put the right configuration that is user, password, host as well as IAM role in dataware house configuration file
3. Run script create_tables.py to create tables if they don't exists already
4. Run etl.py to load data from S3 to staging tables and to insert data from staging tables to analytical tables
