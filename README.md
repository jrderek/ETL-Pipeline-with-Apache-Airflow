# ETL-Pipeline-with-Apache-Airflow

The purpose of the data engineering project is to combine the skills & expertise acquired during the nanodegree-program to accomplish data-engineering tasks such as developing ETL-pipelines with Apache Airflow, handling complex and huge amounts of data with Apache Spark / AWS-EMR, defining efficient star-schema like data-models in AWS-Redshift and utilizing HDFS like storage as AWS-S3. 

The capstone project is the final project to complete the nanodegree-program and deals with four datasets. The main dataset will include data on immigration to the United States, and supplementary datasets will include data on airport codes, U.S. city demographics, and temperature data. The datasets will be ingested and transformed during various steps of a holistic ETL-Pipeline using Apache Airflow, Spark, AWS-S3, AWS-EMR & AWS-Redshift ending up in a final star-schema like data-model which enables to answer analytical questions regarding immigration in the US.

Datasets

The following data was used in this project & was provided by udacity.

I94 Immigration Data: This data comes from the US National Tourism and Trade Office. This is where the data comes from.
World Temperature Data: This dataset came from Kaggle. You can read more about it here.
U.S. City Demographic Data: This data comes from OpenSoft. You can read more about it here.
Airport Code Table: This is a simple table of airport codes and corresponding cities. It comes from here.

Architecture / Technologies
In order to provide a star schema like data-model out of the above stated raw data a clean architecture which accounts for all necessary data-engineering tasks needed had to be created. After an assessment of technologies & requirements, the final architecture was designed as depicted below.

![image](https://user-images.githubusercontent.com/96236642/159076484-725a711e-03b4-425e-92bc-7cb02f4fe517.png)


Screenshot

AWS-S3 will serve as a HDFS like file storage allowing to store huge amounts of data. For the project a S3 bucket was set up with the following structure to separate the data throughout the ETL-Pipeline.

/capstone/data/ingest/ containing all raw data for ingestion
/capstone/data/raw/ containing all ingested raw data of staging-tables (parquet)
/capstone/data/processed/ containing all further processed data of fact-/dimension tables (parquet)
/capstone/data/tmp/ containing temporary data (parquet), gets cleaned-up after each Pipeline run
Data processing including all transformations and ETL tasks is done via Apache Spark. Spark allows for parallel processing of huge amounts of data and provides various capabilities and flexibility when it comes to wrangle big data. In a first step the raw data is ingested into separate staging tables residing as parquet files and Hive tables on S3 and EMR respectively. This step includes basic data clean-up and data-type conversions. The second step creates the first (static) dimension tables of the final star-schema out of provided data mappings. Again, these are stored as parquet files (S3) and Hive tables (EMR) as well as a final dimension table on AWS-Redshift as these tables are utilized for creating further fact- and dimension-tables to complete the star-schema in Redshift. The complete architecture and it's components are fully managed & orchestrated by Airflow-Pipelines running in it's own docker environment. The docker setup is also part of this project and provides a production-ready Airflow setup by also interacting with AWS-S3 as storage for Spark-Jobs and other configuration files that are necessary to provision clusters on AWS.

Airflow-Pipelines
Airflow is running in a custom docker setup where each Airflow-Webserver, Airflow-Scheduler & Airflow-Database (postgres) is running in its own docker-container. Initially we also run an Airflow-initdb container to initialize the database. For more info on how to get this running see the Getting Started section.

01_provision_cluster_dag
This dag is responsible to provision both AWS-Redshift as well as AWS-EMR cluster used to run the workloads and create the data-model. After all tasks are finished the clusters are terminated to not exceed AWS-Budget. 

![image](https://user-images.githubusercontent.com/96236642/159076539-7243b9fa-cd99-4121-b8bd-a0d6e77e1472.png)


Screenshot

02_data_etl_dag
This dag is responsible for the actual workloads / ETL tasks, executing both Spark-Submits on AWS-EMR and read/write operations to AWS-S3 as well as AWS-Redshift. The dag waits until both clusters are provisioned properly and triggers both ingestion of raw data as well as the creation of final fact and dimension tables.

Custom operators update existing airflow connections with the current hosts of provisioned clusters on AWS.
Raw data is ingested via spark-submit & SSH-operator
Static dimensions are created out of data-mappings via spark-submit & SSH-operator
Fact table is created via spark-submit & SSH-operator
Dimension tables are created via spark-submit & SSH-operator
A tmp-directory on AWS-S3 is cleaned up by a custom operator

![image](https://user-images.githubusercontent.com/96236642/159076589-60296cdf-4b35-414c-a621-e3e5c61a59d2.png)

Screenshot

03_data_quality_check_dag
This dag is responsible for assuring data quality of the data residing in the tables of the created data-warehouse. Data quality checks are defined prior to execution and results are compared to an expected result. 

![image](https://user-images.githubusercontent.com/96236642/159076635-f7c2d8f9-2102-4bdb-904e-466320bac04a.png)


Screenshot

Data Model

The project aims to provide an AWS-Redshift data-warehouse with staging tables containing all the data provided for the project by udacity. The following star schema consists of the fact-table fact_immigration and the dimension tables dim_date, dim_state, dim_country, dim_airport, dim_port, dim_travel_mode and dim_visa_type. These tables are a aggregated and combined view of the raw data in the staging tables.

Fact Immigration
This table is created out of a combination of the staging_immigration table / data and the provided dictionaries / static-dimensions i94_states.txt, i94_city_res.txt, dim_travel_mode, dim_port & dim_visa_type.

Table Column	Data Type	Description
id	int	auto generated id, PK
admission_number	int	immigration admission no
cic_id	int	cic number
ins_number	int	ins number
i94_year	int	year of arrival
i94_month	int	month of arrival
arrival_date	date	date of arrival
departure_date	date	date of departure
stayed_days	int	amount of days spent
airline	varchar	airline used
flight_number	varchar	number of flight used
gender	varchar	gender of immigrant
i94_age	int	age of immigrant
year_of_birth	int	birth-year of immigrant
occupation	varchar	occupation of immigrant
i94_city	int	3-digit code of origin city
country_id	varchar	3-digit code of origin country, FK
state_id	varchar	2-character code of target state, FK
port_id	varchar	3-character code of target city, FK
mode_id	varchar	numeric code for type of transport, FK
visa_id	varchar	numeric code for reason of travel, FK
visa_type	varchar	type of visa used


Dimension State
This table is created out of a combination of the staging_demographic table / data and the provided dictionaries / static-dimensions i94_states.txt.

Table Column	Data Type	Description
id	int	auto generated id , PK
state_id	varchar	2-character state-code, FK
state	varchar	name of state
male_population	int	males in city
female_population	int	females in city
total_population	int	total population in city
num_veterans	int	number of veterans in city
foreign_born	int	number of foreign borns in city
american_indian_alaska_native	int	num native population
asian	int	num asian population
african_american	int	num black population
hispanic_latino	int	num hispanic population
white	int	num white population
Dimension Country
This table is created out of a combination of the stag_temperature table / data and the provided dictionaries / static-dimensions i94_city_res.txt.

Table Column	Data Type	Description
id	int	auto generated id, PK
country_id	int	3-digit country code from mapping, FK
country	varchar	country name
average_temperature	float	avg temperature of city
average_temperature_uncertainty	float	avg temperature unc. of city
latitude	float	latitude coordinate
longitude	float	longitude coordinate
Dimension Airport
This table is solely created out of the staging_airport table / data.

Table Column	Data Type	Description
id (PRIMARY_KEY)	int	auto generated id. PK
icao_code	varchar	icao airport code
airport_code	varchar	airport identifier code
state_id	varchar	2-character state-code of airport, FK
country	varchar	2-character country-code of airport, FK
name	varchar	name of airport
type	varchar	airport type
municipality	varchar	city of airport
elevation_ft	int	elevation in feet
gps_code	varchar	gps code
latitude	float	latitude coordinate of airport
longitude	float	longitude coordinate of airport
Dimension Date
This table is solely created out of the staging_immigration table / data and is based on the arrival_date.

Table Column	Data Type	Description
date_id (PRIMARY_KEY)	int	auto generated id
arrival_date	date	arrival date, FK
year	int	4-digit year
month	int	digit month
day	int	digit day
month_string	varchar	3-character month
day_string	varchar	3-character day of week
week	int	digit week of year
day_of_year	int	digit day of year
day_of_week	int	digit day of week
quarter	int	quarter of year
Dimension Port
This table is solely created out of the provided dictionary i94_ports.txt.

Table Column	Data Type	Description
id	varchar	3-character port code from mapping
city	varchar	country name
state_id	varchar	2-character state code from mapping
Dimension Travel-Mode
This table is solely created out of the provided dictionary i94_mode_types.txt.

Table Column	Data Type	Description
id	int	digit-code from mapping
transport	varchar	type of travel
Dimension Visa-Type
This table is solely created out of the provided dictionary i94_visa_types.txt.

Table Column	Data Type	Description
id	int	digit-code from mapping
reason	varchar	type of visa

Getting Started
Get this repository ...
    git clone https://github.com/
Place your aws-key-file.pem into the config directory ...

Adjust the Airflow Variables in config/airlfow_variables.json according to your needs & update the AWS S3-Bucket in scripts/bootstrap/bootstrap_emr.sh

Set your environment variables in start.sh open a Terminal and execute ...

    source set_env.sh
Build the docker container ...
    docker build -f ./Dockerfile -t airflow-base .
Create a clean Airflow-DB (postgres) ...
    docker-compose up -d airflow_db
Start Airflow init-db container to prepare the postgres ...
    docker-compose up airflow_initdb
Start Airflow Webserver & Airflow Scheduler as container ...
    docker-compose up -d airflow_webserver airflow_scheduler
Access Airlfow-UI with localhost:8282 in your browser!

You may change DAG-schedules and run the dags to set up your data-warehouse ...

Possible Scenarios / Further information

How to handle data update frequency / pipeline run interval?
Currently the DAGs are schedule once an hour for testing purposes. Usually the DAGs should run on an interval that
represents the businesses or analyists needs on data actuality. If new data arrives once a day, it would be reasonable
to run the pipeline once a day. Airflow allows to easily adjust DAG schedules according to specific needs.
Data increase by a factor 100x or even more?
Both AWS-EMR/Spark and AWS-Redshift provide strong scaling capabilities in case we have to process a larger amount of
data. We simply could start by utilizing a different `node-type` (Redshift) / `instance-type` (EMR) which provides 
more computing power. Increasing the memory usage and amount of worker nodes in Spark can also help.
Pipeline needs to be run at a specific time on a daily basis?
As stated above Airflow provides strong flexibility in defining custom schedules for running a DAG. This can intuitively
be configured to e.g. 7 a.m. on a daily basis. Airflows SLA feature will also be helpful by sending out alerts if a
pipeline has not finished until a specified time, e.g. 8 a.m. if you want your pipeline to be finished within one hour.
Database needs to be accessed by 100+ people?
After understanding how all these users would query the database it may be sufficient to provide additional &  specific
views which will serve most users queries. Furthermore AWS-Redshift provides elastic resize for a quick adjustment of
performance as well as concurrency scaling. This can significantly boost query performance while many users 
utilizing the cluster. These features can also be applied to specific groups of users or workloads only.

Known Issues
The following issues can arise irregularly and are of unknown nature:

EMR Cluster not started correctly because of AWS-Account restrictions. Please make sure that your Account-Quota does not exceeds any limits.
Unresolved Dependencies at Spark-Submit of packages org.apache.curator_curator-framework-2.7.1, com.databricks.spark-redshift-2.11:2.0.1 & others. This is a Maven/Ivy related issue and after re-submitting via restart of the Airflow task or manual Spark-Submit does fix it.
