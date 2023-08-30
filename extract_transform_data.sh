#!/bin/bash
echo "extract_transform_and_load"

#extract data 
tar -xvzf /home/project/airflow/dags/tolldata.tgz -C /home/project/airflow/dags

#extract data from CSV
cut -d "," -f 1-4 /home/project/airflow/dags/vehicle-data.csv > /home/project/airflow/dags/csv_data.csv

#extract data from TSV
cut -f 5,6,7 /home/project/airflow/dags/tollplaza-data.tsv > /home/project/airflow/dags/tsv_data.csv


#extract data from fixed width file
awk \'{print substr($0, 62, 3) "," substr($0, 66, 5)}\' /home/project/airflow/dags/payment-data.txt > /home/project/airflow/dags/fixed_width_data.csv


#combine files
paste -d "," /home/project/airflow/dags/csv_data.csv /home/project/airflow/dags/tsv_data.csv /home/project/airflow/dags/fixed_width_data.csv > /home/project/airflow/dags/extracted_data.csv

#Transform and load the data
awk -F "," \'{OFS=","; $4 = toupper($4); print}\' /home/project/airflow/dags/extracted_data.csv > /home/project/airflow/dags/staging/transformed_data.csv