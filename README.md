# NBA-Cloud-Data-Lakehouse-Feature-Engineering-Pipeline

📌 Architecture & Tech Stack[ stats.nba.com ] 
       │ (TLS Impersonation via curl_cffi)
       ▼
[ Azure Data Lake Storage Gen2 (Landing Zone) ]
       │ (Raw JSON / Parquet & Trackers)
       ▼
[ DuckDB Feature Engineering Engine ]
       │ (Spatial Metrics: Distance, Angle, Clock Urgency)
       ▼
[ Delta Lake Storage (Gold Layer) ]

Cloud & Storage: Azure Data Lake Storage Gen2 (ADLS Gen2), Delta Lake (deltalake)   

Compute & Query Engine: DuckDB, Pandas   

Extraction & Networking: curl_cffi (Akamai & Cloudflare TLS impersonation), REST APIs   

Data Formats: JSON, Apache Parquet, Delta Format   

✨ Key Features
Robust API Ingestion & Anti-Bot Evasion:
Uses curl_cffi to mimic Chrome TLS signatures (chrome120) to prevent HTTP 403 Forbidden / Cloudflare blocking from stats.nba.com.   
Built-in rate-limiting with randomized sleep jitter to respect API boundaries.   

Cloud Tracker & State Management:
Maintains state trackers (.csv) directly in ADLS Gen2 to allow idempotent pipeline execution and seamless retries for pending game IDs.   

In-Memory Zero-Copy Transformation (DuckDB):
Eliminates the need to download large cloud datasets to disk by querying raw JSON structures in memory via DuckDB.   
Computes spatial features on the fly: Euclidean distance ($d = \frac{\sqrt{x^2 + y^2}}{10}$), shot angles in radians , and quarter time remaining.   

ACID Delta Lake Integration:
Converts engineered tabular datasets into transactional Delta Lake tables with schema merging and automated batch-processing capabilities across entire seasons.   
