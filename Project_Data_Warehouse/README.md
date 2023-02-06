### Project:Data Warehouse

#### Purpose of this db
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