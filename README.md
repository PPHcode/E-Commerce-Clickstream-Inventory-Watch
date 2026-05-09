# E-Commerce Clickstream & Inventory Watch

A Python producer streams synthetic user events for an online electronics store into **Kafka**.
**Spark Structured Streaming** consumes the stream, archives every event to Parquet, and emits a
real-time *Flash Sale* alert whenever a product receives `> 100` views and `< 5` purchases inside a
10-minute sliding window. **Airflow** runs a daily batch DAG over the Parquet archive to produce
a user-segmentation CSV, a top-5-products text report, and a conversion-rate analysis (CSV + PNG).

```
Producer ─▶ Kafka ─▶ Spark Streaming ─▶ Parquet ─▶ Airflow DAG ─▶ Reports
                       │
                       └─▶ flash-sale-alerts topic (real-time)
```

See [architecture.md](architecture.md) for the diagram and tech-stack justification.

---

## Project layout

```
Project/
├── docker-compose.yml             # spins up the whole stack
├── README.md                      # this file
├── architecture.md                # diagram + justification
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── clickstream_producer.py    # synthetic event generator
├── spark/
│   ├── requirements.txt
│   └── streaming_job.py           # Structured Streaming job
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── dags/
│   │   └── daily_segmentation_dag.py
│   └── scripts/
│       ├── _io.py
│       ├── segment_users.py
│       ├── top_products_report.py
│       └── conversion_rate_report.py
├── data/                          # Parquet archive + Spark checkpoints (auto-created)
└── reports/                       # final analytical outputs (auto-created)
```

---

## Prerequisites

1. **Docker Desktop** running on Windows (with WSL2 backend recommended).
2. ~6 GB of free RAM allocated to Docker (Settings → Resources → Memory).
3. Ports **2181**, **9092**, **29092**, and **8080** 
You do **not** need Python, Spark, Kafka, or Airflow installed on your host. Everything runs in
containers.

---

## How to run

Open a terminal in the project root (`e:/Academic 8th Sem/EC8202- Big Data and Analytics/Project`).

### 1. Start the whole stack

```powershell
docker compose up -d --build
```

The first run takes 5-10 minutes (downloading images, building the producer/Airflow images,
installing PySpark Kafka connector). Subsequent runs start in under a minute.

What you just started:

| Container | Purpose | Where to look |
|---|---|---|
| `zookeeper` + `kafka` | Message broker | `docker logs kafka` |
| `kafka-init` | Creates the two topics, then exits | `docker logs kafka-init` |
| `producer` | Pushes ~20 events/sec | `docker logs -f producer` |
| `spark` | Runs the streaming job | `docker logs -f spark` |
| `postgres` | Airflow metadata DB | (internal) |
| `airflow-init` | DB migrate + create admin user, then exits | `docker logs airflow-init` |
| `airflow-webserver` | UI at <http://localhost:8080> (admin / admin) | `docker logs -f airflow-webserver` |
| `airflow-scheduler` | Runs the daily DAG | `docker logs -f airflow-scheduler` |

### 2. Watch the real-time flash-sale alerts

```powershell
docker logs -f spark
```

You'll see normal streaming activity. Every few minutes the producer injects a "view burst" on a
random product - shortly after, Spark prints a `FLASH_SALE_SUGGESTED` alert block to the console
and publishes the same alert as JSON to the `flash-sale-alerts` Kafka topic.

To peek at the alert topic directly:

```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic flash-sale-alerts --from-beginning
```

### 3. Run the Airflow batch DAG

Open <http://localhost:8080> in your browser. Log in with `admin` / `admin`.

You'll see one DAG: **daily_user_segmentation**.

- Toggle it ON (the switch on the left).
- Click the DAG name → **Trigger DAG** (the ▶ play button).

After ~30 seconds the three tasks (`segment_users`, `top5_products_report`,
`conversion_rate_report`) will go green. The outputs land in the `reports/` folder on your host:

```
reports/
├── user_segments_<date>.csv
├── top5_products_<date>.txt
├── conversion_rates_<date>.csv
└── conversion_rates_<date>.png
```

> **Note:** the DAG's scheduled run is daily at 00:30 UTC, but you can trigger it manually any
> time. On a fresh run, if no Parquet partition exists yet for "yesterday", the batch tasks fall
> back to the most recent partition with data, so the demo always produces output.

### 4. Stopping

```powershell
docker compose down               # stop everything, keep Parquet/reports/postgres data
docker compose down -v            # also wipe the postgres metadata volume
```

---

## What each component does (quick map)

- **`producer/clickstream_producer.py`** - generates `view`/`add_to_cart`/`purchase` events at
  ~20/sec for 12 products across 6 categories and 200 users. Occasionally injects a 150-220-event
  view burst on one product so the streaming alert actually fires.
- **`spark/streaming_job.py`** - reads Kafka, writes raw events to Parquet partitioned by date,
  computes a 10-minute sliding window (5-minute slide) with a 2-minute watermark, and emits a
  flash-sale alert when the rule (`views > 100 AND purchases < 5`) holds.
- **`airflow/dags/daily_segmentation_dag.py`** - a daily DAG with three parallel Python tasks
  reading the previous day's Parquet partition.

---

## Troubleshooting

**"Cannot connect to the Docker daemon"** - start Docker Desktop first.

**Producer logs show repeated "Kafka not ready"** - this is expected for the first ~30 seconds
while Kafka boots and topics are created. It self-heals. If it persists for >2 minutes, check
`docker logs kafka` and `docker logs kafka-init`.

**Spark logs show `KafkaConsumer` exceptions on startup** - same root cause as above. The Spark
container has a 30-second sleep before launch, but if your machine is slow you may see one or two
retries before it stabilises.

**Airflow UI says "Broken DAG"** - check `docker logs airflow-scheduler` for the full Python
stack trace.

**Reports folder is empty after running the DAG** - the streaming job hasn't written any Parquet
files yet (look at `data/raw_events/`). Wait a minute or two after first launch and re-trigger
the DAG.

**Port already in use** - either stop whatever is using the port, or edit the host-side port in
`docker-compose.yml` (e.g. change `"8080:8080"` to `"8081:8080"` for Airflow).
