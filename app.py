
import streamlit as st
from google.cloud import storage
import uuid
import json
import time
import os
import subprocess

PROJECT_ID = "your-gcp-project-id"
BUCKET_NAME = "job-title-predict-bucket"
INPUT_PATH = "job-description-inputs"
OUTPUT_PATH = "job-results"
DATAPROC_CLUSTER = "your-dataproc-cluster"
REGION = "us-central1"
PYSPARK_SCRIPT_GCS = f"gs://{BUCKET_NAME}/job_title_pipeline.py"

st.title("🔍 Job Title Prediction")

desc = st.text_area("📝 Enter job description:", "Looking for a backend developer with cloud experience")

if st.button("🚀 Predict Job Title"):
    uid = uuid.uuid4().hex[:8]
    input_blob = f"{INPUT_PATH}/input_{uid}.txt"
    output_blob = f"{OUTPUT_PATH}/output_{uid}.json"
    local_input = f"/tmp/input_{uid}.txt"

    with open(local_input, "w") as f:
        f.write(desc)

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    bucket.blob(input_blob).upload_from_filename(local_input)

    # Submit Dataproc Job
    cmd = [
        "gcloud", "dataproc", "jobs", "submit", "pyspark", PYSPARK_SCRIPT_GCS,
        "--cluster", DATAPROC_CLUSTER,
        "--region", REGION,
        "--project", PROJECT_ID,
        "--",
        "--mode=predict",
        f"--input=gs://{BUCKET_NAME}/{input_blob}",
        f"--output=gs://{BUCKET_NAME}/{output_blob}"
    ]

    st.info("📡 Submitting job to Dataproc...")
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        st.error("❌ Job submission failed.")
        st.code(e.stderr)
        st.stop()

    # Wait for result
    st.info("⏳ Waiting for result...")
    for _ in range(30):
        if storage.Blob(bucket=bucket, name=output_blob).exists(client):
            content = bucket.blob(output_blob).download_as_text()
            result = json.loads(content)
            st.success(f"🔮 Predicted Title: {result['predicted_title']}")
            for i, job in enumerate(result["top_similar"], 1):
                st.markdown(f"**{i}. {job['job_title']}** — {job['company']} ({job['sector']} > {job['industry']})<br>"
                            f"📍 {job['location']}, {job['country']} | 💰 {job['salary_range']}<br>"
                            f"🎓 {job['qualifications']} | 🔗 Similarity: {job['cosine']}", unsafe_allow_html=True)
            break
        time.sleep(3)
    else:
        st.error("❌ Timeout: Prediction not received.")

    os.remove(local_input)
