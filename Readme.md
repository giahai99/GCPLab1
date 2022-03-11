##GCP - Lab 1 - Johnny

###1. Open GCP Cloud Shell

###2. Clone/Pull the code from the respository to the cloud shell on GCP
```
git clone https://github.com/giahai99/GCPLab1.git
```

###3. Publish message to Pub/Sub topic input

Step 1: Install python environment and dependencies
```
pip3 install faker
pip3 install google-cloud-pubsub
```
Step 2: Run file publish
```
python3 publish_data.py
```

###4. Submit job and run dataflow
Step1: Resolve dependencies
```
cd GCPLab1
export BASE_DIR=$(pwd)
mvn clean dependency:resolve
```
Step 2: Run dataflow pipeline
```
cd $BASE_DIR
export LAB_ID=40
export PROJECT_ID=$(gcloud config get-value project)
export MAIN_CLASS_NAME=com.nttdata.gcp.PubSubToBigQuery
export PUBSUB_TOPIC_INPUT=projects/${PROJECT_ID}/topics/uc1-input-topic-$LAB_ID
export PUBSUB_TOPIC_OUTPUT=projects/${PROJECT_ID}/topics/uc1-dlq-topic-$LAB_ID
export OUTPUT_BIGQUERY_TABLE=${PROJECT_ID}:uc1_$LAB_ID.account
export REGION=europe-west4

mvn compile exec:java \
-D exec.mainClass=${MAIN_CLASS_NAME} \
-D exec.args=" \
--inputTopic=${PUBSUB_TOPIC_INPUT} \
--outputTopic=${PUBSUB_TOPIC_OUTPUT} \
--outputTable=${OUTPUT_BIGQUERY_TABLE} \
--project=${PROJECT_ID} \
--region=${REGION} \
--runner=DataflowRunner \
--jobName=usecase1-labid-$LAB_ID \
--serviceAccount=Haidepzai_kut3@yahoo.com \
--maxNumWorkers=1 \
--workerMachineType=n1-standard-1 \
--gcpTempLocation=gs://c4e-uc1-dataflow-temp-$LAB_ID/temp \
--stagingLocation=gs://c4e-uc1-dataflow-temp-$LAB_ID/staging \
--subnetwork=regions/europe-west4/subnetworks/subnet-uc1-$LAB_ID --streaming"
```