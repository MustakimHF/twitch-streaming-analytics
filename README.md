# 🎮 Twitch Streaming Analytics (with Airflow ETL)

A **data engineering and analytics project** that ingests live data from the **Twitch API**, processes it with **Apache Airflow**, and stores insights in a **Postgres database**.  
It demonstrates **ETL (Extract–Transform–Load)** pipelines, **data quality checks**, **trend analysis**, and **visualisation** — finished with stakeholder-ready outputs.  

---

## 🚀 What This Project Does  

- 📡 **Extracts live data** from Twitch (stream metadata, game categories, viewers)  
- ⚙️ **Automates ETL pipeline** with **Airflow DAGs** (scheduled runs, retries, monitoring)  
- 🧹 **Cleans and validates** data (deduplication, UTF-8 handling, schema enforcement)  
- 🗄️ **Loads into Postgres** for structured analytics  
- 📊 **Analyses streaming trends** by game, category, and viewers  
- 📈 **Produces visuals & summaries** (most-watched games, viewer trends, game diversity)  

---

## 🧰 Tech Stack  

- **Data Orchestration**: Apache Airflow (Docker Compose)  
- **Database**: PostgreSQL (with SQLAlchemy + psycopg2)  
- **Python**: `pandas`, `numpy`, `requests`, `python-dotenv`  
- **Visualisation**: `matplotlib`  
- **Containerisation**: Docker & docker-compose  
- **Config Management**: `.env` for Twitch API secrets  

---

## 📁 Repository Structure  

```
twitch-streaming-analytics/
├── README.md                   # Project overview (this file)
├── docker-compose.yaml          # Services: Airflow + Postgres
├── dags/
│   ├── twitch_etl_dag.py       # Main Airflow DAG
│   ├── extract_twitch.py       # Extract: Twitch API calls
│   ├── transform.py            # Transform: clean & enrich data
│   └── load.py                 # Load: push to Postgres
├── db/
│   └── init.sql                # Database schema (streams table)
├── data/                       # Raw + processed CSVs
├── outputs/
│   ├── plots/                  # Generated plots (PNG)
│   └── reports/                # Aggregated summaries
├── .env.example                # Example environment variables
└── requirements.txt            # Python dependencies
```

---

## ▶️ How to Run  

### 1. Clone repo & create `.env`  

```bash
cp .env.example .env
```

Fill in:  
```env
TWITCH_CLIENT_ID=your_twitch_client_id
TWITCH_CLIENT_SECRET=your_twitch_client_secret
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=twitch
```

---

### 2. Start services with Docker  

```bash
docker compose up -d
```

- Airflow web UI → [http://localhost:8080](http://localhost:8080)  
- Postgres DB → `localhost:5432`  

Login to Airflow:  
- **Username**: `airflow`  
- **Password**: `airflow`  

---

### 3. Trigger the ETL DAG  

- In Airflow UI, enable `twitch_etl_dag`  
- Monitor tasks: `extract → transform → load`  
- Data lands in Postgres `streams` table  

---

### 4. Explore & Analyse  

Run analysis scripts or notebooks:  

```bash
python analysis/top_games.py
```

Generates plots in `outputs/plots/`, e.g.:  

- 📊 **Top 10 games by average viewers**  
- ⏰ **Viewership trends over time**  
- 🎯 **Category diversity**  

---

## 📊 Example Visuals  

**Top 10 Games by Average Viewers**  
![Top Games](assets/plots/top_games.png)  

**Viewer Trends Over Time**  
![Viewership Trends](assets/plots/viewer_trends.png)  

---

## 🎯 Why This Project Matters  

This project showcases **real-world data engineering**:  

- **ETL pipelines** with Airflow for reliability, retries, and scheduling  
- **API integration** with Twitch’s OAuth flow and rate limits  
- **Database design & SQL analytics** for structured insights  
- **Exploratory analysis & visualisation** for stakeholder reporting  
- **Containerised deployment** for reproducibility (Docker)  

📌 *This mirrors the workflow of data engineer / data analyst roles in gaming, media, and live-streaming analytics — ideal for portfolios and interviews.*  

---

## 🔒 Notes  

- `.env` is git-ignored — never commit secrets  
- Airflow uses a Postgres backend (persistent via Docker volume)  
- Extend project: add sentiment analysis on Twitch chat logs, or track streamer growth over time  

---

## 📄 Licence  

MIT Licence – free to use and adapt.  
