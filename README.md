## Project Outline
This project is intended to build an end-to-end data pipeline of PV Rooftop data to visualize the zip codes with the highest density of buildings suitable for solar panels.

* GCP Infrastructure built using Terraform
* Docker container runs Airflow
* Extract and load data using Airflow
* Store data in GCP and query using BigQuery
* Transformation and modeling on BigQuery with DBT
* Visualize data on a dashboard from BigQuery using DataStudio

Each step listed above can be found in the corresponding folders.

## The Dataset

The National Renewable Energy Laboratory's (NREL) PV Rooftop Database (PVRDB) is a lidar-derived, geospatially-resolved dataset of suitable roof surfaces and their PV technical potential for 128 metropolitan regions in the United States. The source lidar data and building footprints were obtained by the U.S. Department of Homeland Security Homeland Security Infrastructure Program for 2006-2014. Using GIS methods, NREL identified suitable roof surfaces based on their size, orientation, and shading parameters Gagnon et al. (2016). Standard 2015 technical potential was then estimated for each plane using NREL's System Advisory Model.
