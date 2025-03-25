## prepare data  
running the code in pyspark.ipynb  
## upload parquet data to google storage  
gsutil -m cp -r ASBO gs://dtc-de-course-448111-kestra/pq/ASBO  
## running spark job and save data to BigQuery  
gcloud dataproc jobs submit pyspark \  
    --cluster=de-zoomcamp-cluster \  
    --region=europe-west2 \  
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \  
    gs://dtc-de-course-448111-kestra/code/spark_sql_big_query.py \  
    -- \  
        --input_ASBO=gs://dtc-de-course-448111-kestra/pq/ASBO/2001-2013/* \  
        --output=ASBO_data.reports-2001-2013  
## studio report  
https://lookerstudio.google.com/s/s4YPYDr--oM