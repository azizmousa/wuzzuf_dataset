# Introduction:
---------------------------------
this application is java web applicatoin that use spark library to work with wuzzuf dataset and do some operations on it.


# Instructions:
---------------------------------
- To use this application load the application and run it on application server prefared "APACHE TOMCAT"
- use intellij ide to load the project and run it
- open localhost:8080/
- folow links to apply specific operation


# API Documentation:
----------------------------------
- To load csv dataset : "http://localhost:8080/load-csv?path=[dataset-path]&header=[is_header?]"
	- Default dataset: http://localhost:8080/load-csv?path=src/main/resources/Wuzzuf_Jobs.csv&header=true

- To Print Summary of loaded DS: http://localhost:8080/summary

- To Coun number of nulls in each column: http://localhost:8080/count-null

- To Remove Rows That have null values: http://localhost:8080/drop-null

- To Remove all duplicated columns: http://localhost:8080/remove-dup

- To Print all Dataset: http://localhost:8080/print-df

- To Draw Most Demanded companies Pi Chart: http://localhost:8080/most-demanded-jobs-pie

- To Draw Most Popular Jobs Bar Chart: http://localhost:8080/most-jobs-title-bar

- To Draw Most Popular Areas Bar Chart: http://localhost:8080/most-pop-areas-bar

- To Draw Most Popular Skills Bar Chart: http://localhost:8080/most-pop-skills-bar

- To Factorize YearsExp: http://localhost:8080/convert-years-exp

- To Run pipline and run all previouse operations : http://localhost:8080/pip-line?path=[dataset-path]&header=[is_header?]
	- Default dataset: http://localhost:8080/pip-line?path=src/main/resources/Wuzzuf_Jobs.csv&header=true

