# 🌆 ZenClarity-UrbanFlow — NYC Taxi Data Engineering Project

**ZenClarity-UrbanFlow reflects my approach to data engineering: clarity, scalability, and modern cloud practices.**  
Built with AWS, dbt, Redshift, and Snowflake, it showcases both batch and streaming pipelines, automated orchestration, and a real-time analytics dashboard.

- 🚖 **Pipelines**: Real-time streaming (Kinesis) + batch ingestion (Glue)  
- 📦 **Warehousing**: Redshift Serverless & Snowflake  
- 📊 **Modeling**: dbt (staging → intermediate → marts)  
- ⚡ **Benchmarks**: Redshift vs Snowflake performance  
- 📈 **Visualization**: Streamlit dashboard (screenshots included)  

---

## 📊 Project Highlights

- **Data Ingestion**: Dual-pipeline system — Python-based simulator for live events via **Kinesis Firehose** and AWS Glue for scheduled batch ingestion.  
- **Data Lake & Storage**: Central **Amazon S3** data lake; **DynamoDB** used for observability and audit logging.  
- **Data Transformation**: ETL with **AWS Glue** and ELT with **dbt** (multi-layer: staging, intermediate, marts).  
- **Data Warehousing**:  
  - **Redshift Serverless** for streaming & batch analytics.  
  - **Snowflake** for bulk load + benchmarking.  
- **Orchestration**: **AWS Step Functions** automate and manage pipeline workflows.  
- **Visualization**: **Streamlit dashboard** surfaces KPIs and real-time vs baseline comparisons.  
- **Cross-Database Compatibility**: Custom **dbt macros** ensure the same models run seamlessly on both Redshift and Snowflake, enabling apples-to-apples benchmarking and portable analytics code.  

> Note: Streaming pipeline operates near-real-time (sub-minute latency) using fully managed AWS services.  


---

## 🗺️ Architecture

![Architecture Diagram](docs/arch_diagrams/ZenClarity-UrbanFlow_architecture.jpg)

---

## 📂 Repo Structure 

ZenClarity-UrbanFlow/  
├── analytics/          # notebooks + Streamlit app (screenshots archived)  
├── config/             # sample env/config snippets  
├── dbt/                # dbt models (staging → intermediate → marts)  
├── docs/               # diagrams, metrics, planning notes  
├── infrastructure/     # EMR, Glue, Redshift, Snowflake configs  
├── scripts/            # ETL code (batch, streaming, emr_jobs, helpers)  
└── venv/               # local virtualenv (ignored in git)  

---

## 📈 Data Models & dbt

This project follows a **multi-layered dbt modeling pattern**:
- **Staging**: cleans raw data, enforces schema.  
- **Intermediate**: joins + transformations for readability/efficiency.  
- **Marts**: business-defined entities for analytics (facts & dims).  

📑 **Documentation & lineage**:  
[View dbt Project Documentation (S3 Hosted)](http://nle-dbt-docs.s3-website-us-east-1.amazonaws.com/#!/overview)

---

## 📊 Performance Benchmarks

A representative query pack was executed on both **Redshift** and **Snowflake** using dbt-built schemas.  

- ✅ Filters & partition pruning  
- ✅ Aggregations & group-by  

Results highlight performance trade-offs between Redshift and Snowflake under equivalent schema and query conditions:

![Redshift vs Snowflake Benchmark](docs/metrics/Snowflake_vs_Redshift_Benchmark.jpg)

---

## 📈 Dashboard KPIs (Streamlit)

* Trip count  
* Total fare revenue  
* Average trip delay  
* Passengers carried  
* Trips per minute  
* Real-time vs baseline comparison  
* Cumulative trip chart  

**Screenshots:**  
![Dashboard KPIs](docs/metrics/Streaming-KPI.jpg)  
![Dashboard Screenshot](docs/metrics/Streaming-Dashboard-1.jpg)

---

## 🌐 Technologies Used

**AWS Services:**  
S3 • Kinesis Firehose • Glue • Lambda • Step Functions • EventBridge • DynamoDB • Athena • Redshift Serverless  

**Other Tools:**  
dbt • Snowflake • Python • Streamlit  

---

## 📚 Roadmap

- **Next Phase: EMR Migration**  
  - Re-architect batch processing with Spark & Hive (EC2 + EMR Serverless).  
  - Compare cost & performance vs AWS Glue.  
  - Extend docs with tuning experiments.  

- **Future Enhancements**  
  - Iceberg tables / Athena for streaming cost optimization  
  - Predictive analytics (surge demand zones)  
  - More realistic simulation from historical patterns  

---

## 💡 Inspiration

> *"Modern Data Engineering: Combining batch + streaming for near real-time decision making."*

---

## 🔗 Connect

- LinkedIn: [le-nguyen-v](https://www.linkedin.com/in/le-nguyen-v/)  
- GitHub: [tropily](https://github.com/tropily/Zen_Clarity)

