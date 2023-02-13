# Covid19 and Vaccination Analysis

# Index
- [Problem Description](#problem-description)
- [Dataset](#dataset)
- [Technologies Used](#technologies-used)
- [Steps for Project Reproduction](#steps-for-project-reproduction)
- [Summary of DAG of Data Pipeline and decisions regarding transformations](#Summary-of-DAG-of-Data-Pipeline-and-decisions-regarding-transformations)
- [Dashboard](#dashboard)
- [Conclusion](#conclusion)

# Problem Description

The purpose of this project is to analize the COVID-19 and Vaccination state of the country over the world

Brief summary of the steps:
* Download csv data file from WHO site
* Upload of csv data file into a Data Lake. 
* Convertion of data from csv to parquet, transformation using PySpark and upload to Data Warehouse. 
* Creation of dashboard to analyse data and visualize results.

# Dataset

The dataset is downloaded daily from WHO site.
* Covid-19 cases: https://covid19.who.int/WHO-COVID-19-global-data.csv
* Covid-19 vaccinations: https://covid19.who.int/who-data/vaccination-data.csv

# Technologies Used

For this project I decided to use the following tools:
- **Infrastructure as code (IaC):** Terraform
- **Workflow orchestration:** Airflow
- **Containerization:** Docker
- **Data Lake:** Google Cloud Storage (GCS)
- **Data Warehouse:** Google BigQuery
- **Transformations:** PySpark 
- **Visualization:** Google Data Studio

# Steps for Project Reproduction

## Step 1
Creation of a [Google Cloud Platform (GCP)](https://cloud.google.com/) account.

**Note:** You should have a valid credit/debit card to use 300$ free credit 

## Step 2: Setup of GCP 
- Creation of new GCP project. Attention: The Project ID is important. 
- Go to `IAM & Admin > Service accounts > Create service account`, provide a service account name and grant the roles `Viewer`, `BigQuery Admin`, `Storage Admin`, `Storage Object Admin`,`Dataproc Admin`, `Dataproc API` 
- Download your key locally, rename it to `google_credentials.json`. 
- Store it in your home folder `$HOME/.google/credentials/`for easier access. 
- Set as the variable GOOGLE_APPLICATION_CREDENTIALS
```
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"; 
```
- Activate the following API's:
   * https://console.cloud.google.com/apis/library/iam.googleapis.com
   * https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com

## Step 3: Creation of a GCP Infrastructure
- [Install Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli)
- Change default variables `project`, `region`, `BQ_DATASET` in `variables.tf` (the file contains descriptions explaining these variables)
- Run the following commands on bash:

```shell
# Initialize state file (.tfstate)
terraform init

# Check changes to new infrastructure plan
terraform plan

# Create new infrastructure
terraform apply
```

## Step 4: Use of DockerFile and Docker-Compose structure to run Airflow.
### Installation
You can follow this [link](https://airflow.apache.org/docs/apache-airflow/2.1.1/start/docker.html) to run Airflow in Docker
### Execution

**0.** In `docker-compose.yaml`, change the environment variables `GCP_PROJECT_ID`,`GCP_GCS_BUCKET` and the line  `/home/username/.google/credentials/` regarding the volume to your own setup values. 

**1.** Build the image (may take several minutes). You only need to run this command if you modified the Dockerfile or the `requirements.txt` file or if the first time you run Airflow. 

```
docker-compose build
```
    
**2.** Initialize the configurations:

```
docker-compose up airflow-init
```
    
**3.** Run Airflow:

```
docker-compose up
```
    
**4.** Browse `localhost:8080` to access the Airflow web UI. If you are using SSH, you may need to forward port to your local port on VSC.

**5.** Turn on the DAG and trigger it on the web UI or wait for its scheduled run (once a day). After the run is completed, shut down the container by running the command:

```
docker-compose down
```
