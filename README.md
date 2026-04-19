# CS498D HW4 - Neo4j, PySpark, FoundationDB

Submission for `abach8`.

## Live endpoint

The Flask server is running at:

```
http://136.115.86.160:5000
```

(see `HW4.txt`).

## Contents

| File | Purpose |
| --- | --- |
| `Team.txt` | NetID(s) |
| `HW4.txt` | External IP of the GCP VM (`<ip>:5000`) |
| `clean.py` | Dataset cleaning script (verbatim from spec) |
| `load_graph.py` | Loads `taxi_trips_clean.csv` into Neo4j |
| `preprocess.py` | Part 2.1 PySpark preprocessing -> `processed_data/` |
| `app.py` | Flask app exposing all 8 endpoints (5 Neo4j + 3 PySpark) |
| `requirements.txt` | Python dependencies |
| `fdb_answers.txt` | Part 3 FoundationDB answers |
| `Design.pdf` | Part 4 written analysis |
| `screenshots/processed_data.png` | Required Part 2.1 screenshot |

## How to reproduce

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 1. clean
gdown 1JUxPihSobTdlL4UCbxhL2qBP7HMz-ntW -O taxi_trips.csv
python clean.py

# 2. load Neo4j
N_PWD=hw4password python load_graph.py

# 3. PySpark preprocessing (writes processed_data/)
python preprocess.py

# 4. start API
N_PWD=hw4password python app.py
```

## Endpoints

### Neo4j (Part 1)

- `GET /graph-summary`
- `GET /top-companies?n=<int>`
- `GET /high-fare-trips?area_id=<int>&min_fare=<float>`
- `GET /co-area-drivers?driver_id=<string>`
- `GET /avg-fare-by-company`

### PySpark (Part 2.2)

- `GET /area-stats?area_id=<int>`
- `GET /top-pickup-areas?n=<int>`
- `GET /company-compare?company1=<string>&company2=<string>`

## Part 2.1 Screenshot

`processed_data/` folder contents (including `_SUCCESS`) and the head of one part file:

![processed_data screenshot](screenshots/processed_data.png)

## Stack

- Ubuntu 22.04 LTS on GCP `e2-standard-2`
- OpenJDK 21 (Temurin)
- Neo4j Community 5.x
- Python 3.10 + PySpark 3.5.1 (local mode), Flask 3, neo4j 5
