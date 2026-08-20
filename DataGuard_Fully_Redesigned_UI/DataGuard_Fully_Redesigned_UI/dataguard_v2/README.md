# DataGuard Studio V2

Flask + HTML/CSS/JavaScript demo for a Cognizant data integration / validation hackathon.

## Run on Windows PowerShell

```powershell
pip install -r requirements.txt
python app.py
```

Open:

```text
http://127.0.0.1:5000
```

## Main features

- CSV / Excel / JSON ingestion
- Data profiling and column visualizations
- Dynamic per-column validation builder
- Composite duplicate rules using multiple selected columns
- Default validation vs temporary custom validation preview vs combined validation
- Temporary valid / invalid datasets with downloads
- DQ breakdown: Completeness, Validity, Uniqueness, Consistency, Integrity
- Dataset-specific DQ improvement tips
- Kafka streaming monitor UI with offsets, partitions and lag simulation
- Spark-style partition viewer with sequential or hash-by-column repartition preview
- Performance metrics: ingestion time, validation time, rows/sec, memory and skew ratio
- Export Center for original, clean, valid, invalid, temporary results, profile, DQ, rules, issues and audit history
- Complete ZIP export bundle
- Floating DataGuard chatbot using SQL retrieval over the current dataset plus a small RAG-style data-quality knowledge base

## Important hackathon note

Kafka and Spark metrics are intentionally labelled as a demo/simulation in this version. The UI/backend boundaries are already separate so real Kafka and PySpark can be plugged into the same endpoints later.

The chatbot is fully local and does not require an API key. It uses deterministic SQL retrieval for supported dataset questions and keyword retrieval from an internal data-quality knowledge base. A real LLM can be added later without changing the dashboard design.
