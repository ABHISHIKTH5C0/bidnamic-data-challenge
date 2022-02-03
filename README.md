# bidnamic-data-challenge
Bidnamic's Data Engineering Coding Challenge 
<h3>For this task i designed following flow chart of ETL pipeline as per task requirment</h3>

<img>ETL_flow_chart.jpg</img>
<h5>**This flow is desined by considering good practices</h5>
<ul>
  <li>Ingested data from raw url files
  <a href="https://raw.githubusercontent.com/bidnamic/bidnamic-data-challenge/master/campaigns.csv">campaings</a>,
    <a href="https://raw.githubusercontent.com/bidnamic/bidnamic-data-challenge/master/adgroups.csv"> adgroups</a>,
     <a href="https://raw.githubusercontent.com/bidnamic/bidnamic-data-challenge/master/search_terms.csv"> search_terms</a>
     into AWS S3 which considered as data lake with folowing <a href="">Python code</a>.
  </li>
  <li>
  Extracted the ingested data objects AWS S3 bucket using <a href='https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html'>boto3</a>
  with the following python Script in <a href="ETL_Pipeline_Bidnamic_task-checkpoint.ipynb">jupyter Notebook </a> 
  by this we can perform the tranfomations and data processing accroding to the business requirement.
  </li>
  
  <li>
  In the data preprocessing steps for good practice while stroing extracted data into the postgreSQL we created data frames using the pyspark 
  <a href="ETL_Pipeline_Bidnamic_task-checkpoint.ipynb">jupyter Notebook </a>
  and  inserted additional columns which are "created_timestamp", "created_by" and "modified_timestamp" by these columns we can track the data flow 
  while extracting the data from source.
  </li>
  <li>
  while data preprocessing i considered to store the data in three diffirent following levels of schema's which are
    <br>
    <ol> 1. Stagedata schema</ol>
    <ul>- Raw data is loaded into this schema and we assign data types as per data. </ul>
    <br>
    <ol> 2. Storedata schema</ol>
    <ul>- In this level we will load clean data by filtering the duplicates and creat the distinct value data frame with transformations as per business requiremtn.</ul>
    <br>
    <ol> 3. Reportdata schema </ol>
    <ul>- This schema contains the completed busness required data tables.</ul>
    <br>
   by segregating schema's in postgreSQL database we can track the data utlisation as per busness requirement.
  </li>
  <li>These schema's created by using the <a href='https://pypi.org/project/psycopg2/'>psycopg2</a> library where it used to connect the PgAdmin with configuration and
  it helps to execute the SQL statments</li>
  <li>Data loading is done by configuring the <a href='https://jdbc.postgresql.org/'> postgreSQL jdbc driver</a></li>
  <li>For entire cofiguration process like AWS key acess, postgreSQL connection and other input paths are used by loading <a href=''>aws_config.json </a></li> file 
  where this method is considered as good practice
</ul>

<h3>Source code files</h3>
  <ul>
  <li><a href='ingest.py'>ingest.py</a></li>
  <li><a href='ETL_Pipeline_Bidnamic_task-checkpoint.ipynb'>ETL_Pipeline_Bidnamic_task-checkpoint.ipynb</a></li>
  <li><a href='aws_cofig.json'>aws_cofig.json</a></li>
  </ul>
 
<h3>JAR FILES</h3>
These file used for the extracting the S3 bucket files to the loacal envieronent for this we need hadoop campatibility jar files
<ul>
<li>hadoop-aws-3.3.1</li>
<li>aws-java-sdk-bundle-1.11.901</li>
</ul>
<h3>SCRIPTING LANGUAGES</h3>
<ul>
<li>Python</li>
<li>postgreSQl</li>
<ul/>

