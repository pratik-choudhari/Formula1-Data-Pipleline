# Formula1-Data-Pipeline
An end-to-end formula1 data pipeline built with Azure Databricks, Azure Data Factory, and Apache Spark.

<p align="center">
    <img height=300 src="images/f1_car.jpg"/>
</p>

## Table of Contents

1. [Architecture](#architecture)
2. [Pipelines](#data-factory-pipelines)
3. [Trigger](#trigger)
4. [Sample Trigger Execution](#triggered-execution-video)

## Architecture

<p align="center">
   <img src="images/formula1-pipeline-architecture.png"/>
</p>

### Data flow:
1. Ingestion
   - For every F1 race, a new folder is generated in the raw data layer, containing 8 files specific to that race on a given date.
   - Azure Data Factory periodically monitors for the arrival of new data and, once detected, triggers a notebook pipeline. The raw data is then moved to the processed layer in the Delta Lake.
2. Transformation
   - Once the raw data is processed, a transformation pipeline is activated.
   - The transformation notebooks join, filter, and store the processed data in the presentation layer of the Delta Lake.
3. Analysis
   - Data can be queried using SparkSQL or the DataFrame API for flexible data exploration.
   - Visualization tools such as Power BI can connect to the Delta Lake to create dashboards and reports.
<br>

## Data Factory Pipelines

### Main Pipeline

<img width="100%" align="center" src="images/f1_pipeline.png"/>

### Ingestion Pipeline

<img width="100%" align="center" src="images/ingestion_pipeline_flow.png"/>

Debugging the pipeline:

<img width="100%" align="center" src="images/ingestion_pipeline_debug_ss.png"/>

#### Activities:

1. Get Metadata
   - Accesses blob storage via linked service to check if a folder for current run exists in raw container.
   - Output consists of an Exists flag.
2. If Condition
   - Takes the output from Get metadata activity.
   - If output.exists flag is true then execute the following pipeline. All files available [here.](./ingestion)
     <img width="100%" align="center" src="images/ingestion_pipeline_if_true.png"/>
   - Else trigger a logic app which send a failure email to a specific email id.<br>
      <div style="display: flex; flex-direction: row; justify-content: space-evenly">
         <img height=200 src="images/ingestion_pipeline_if_false.png"/>
         <img height=200 src="images/email_logic_app.png"/>
      </div>
   - Error email<br>
      <img height=200 align="center" src="images/pipeline_failure_email.png"/>

### Transformation Pipeline

<img width="100%" align="center" src="images/transformation_pipeline_flow.png"/>

#### Activities:

1. Databricks Notebook
   - Execute the [race_results](./transform/race_results.py) databricks notebook to generate race_results delta table in presentation layer.
   - This new table is created by joining and filtering data from 4 processed layer tables namely races, circuits, drivers and constrcutors.
   - Information in this table can be used to show a dashboard of results after each race.
2. Databricks Notebook
   - Execute the [driver_standings](./transform/driver_standings.py) databricks notebook to generate driver_standings delta table in presentation layer.
   - Data is pulled from race_results table to generate driver standings, hence this activity must run only after successful execution of race results transformation.

## Trigger

<img width="100%" align="center" src="images/trigger.png"/>

A tumbling windows trigger with 168hrs/2w interval, end date specified and max concurrency set to 1.

Parameter configuration:
```json
{
  "p_window_end_date": "@trigger().outputs.windowEndTime"
}
```


## Triggered Execution (Video)

[![ADF trigger](https://img.youtube.com/vi/f-cbAJRQi1E/0.jpg)](https://www.youtube.com/watch?v=f-cbAJRQi1E)