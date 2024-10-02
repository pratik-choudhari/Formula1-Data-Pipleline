# Formula1-Data-Pipeline
An end-to-end formula1 data pipeline built with Azure Databricks, Azure Data Factory, and Apache Spark.

<p align="center">
    <img height=300 src="images/f1_car.jpg"/>
</p>

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

### Ingestion Pipeline

<img width="100%" align="center" src="images/ingestion_pipeline_flow.png"/>

#### Activities:

1. Get Metadata
   - Accesses blob storage via linked service to check if a folder for current run exists in raw container.
   - Output consists of an Exists flag.
2. If Condition
   - Takes the output from Get metadata activity.
   - If output.exists flag is true then execute the following pipeline
     <img width="100%" align="center" src="images/ingestion_pipeline_if_true.png"/>
   - Else trigger a logic app which send a failure email to a specific email id.<br>
      <div style="display: flex; flex-direction: row; justify-content: space-evenly">
         <img height=200 src="images/ingestion_pipeline_if_false.png"/>
         <img height=200 src="images/email_logic_app.png"/>
      </div>
   - Error email<br>
      <img height=200 align="center" src="images/pipeline_failure_email.png"/>

### Transformation Pipeline

TBD