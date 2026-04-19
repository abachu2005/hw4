import os
from flask import Flask, request, jsonify
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, lit, desc, round as rnd

U = os.environ.get("N_URI", "bolt://localhost:7687")
N = os.environ.get("N_USER", "neo4j")
P = os.environ.get("N_PWD", "hw4password")
F = os.environ.get("CSV", "taxi_trips_clean.csv")

g = GraphDatabase.driver(U, auth=(N, P))

s = (
    SparkSession.builder
    .appName("hw4-api")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
s.sparkContext.setLogLevel("WARN")

d = s.read.csv(F, header=True, inferSchema=True).withColumn(
    "fare_per_minute", col("fare") / (col("trip_seconds") / lit(60.0))
).cache()
d.count()

app = Flask(__name__)


@app.get("/graph-summary")
def r1():
    with g.session() as x:
        a = x.run("MATCH (y:Driver) RETURN count(y) AS v").single()["v"]
        b = x.run("MATCH (y:Company) RETURN count(y) AS v").single()["v"]
        c = x.run("MATCH (y:Area) RETURN count(y) AS v").single()["v"]
        e = x.run("MATCH ()-[y:TRIP]->() RETURN count(y) AS v").single()["v"]
    return jsonify({"driver_count": a, "company_count": b, "area_count": c, "trip_count": e})


@app.get("/top-companies")
def r2():
    n = int(request.args.get("n", 5))
    q = (
        "MATCH (a:Driver)-[:WORKS_FOR]->(b:Company), (a)-[:TRIP]->() "
        "WITH b.name AS name, count(*) AS trip_count "
        "RETURN name, trip_count ORDER BY trip_count DESC LIMIT $n"
    )
    with g.session() as x:
        o = [{"name": z["name"], "trip_count": z["trip_count"]} for z in x.run(q, n=n)]
    return jsonify({"companies": o})


@app.get("/high-fare-trips")
def r3():
    a = int(request.args.get("area_id"))
    f = float(request.args.get("min_fare"))
    q = (
        "MATCH (b:Driver)-[t:TRIP]->(c:Area {area_id: $a}) "
        "WHERE t.fare > $f "
        "RETURN t.trip_id AS trip_id, t.fare AS fare, b.driver_id AS driver_id "
        "ORDER BY fare DESC"
    )
    with g.session() as x:
        o = [{"trip_id": z["trip_id"], "fare": z["fare"], "driver_id": z["driver_id"]} for z in x.run(q, a=a, f=f)]
    return jsonify({"trips": o})


@app.get("/co-area-drivers")
def r4():
    i = request.args.get("driver_id")
    q = (
        "MATCH (a:Driver {driver_id: $i})-[:TRIP]->(b:Area)<-[:TRIP]-(c:Driver) "
        "WHERE c.driver_id <> $i "
        "WITH c.driver_id AS driver_id, count(DISTINCT b) AS shared_areas "
        "RETURN driver_id, shared_areas ORDER BY shared_areas DESC"
    )
    with g.session() as x:
        o = [{"driver_id": z["driver_id"], "shared_areas": z["shared_areas"]} for z in x.run(q, i=i)]
    return jsonify({"co_area_drivers": o})


@app.get("/avg-fare-by-company")
def r5():
    q = (
        "MATCH (a:Driver)-[:WORKS_FOR]->(b:Company), (a)-[t:TRIP]->() "
        "WITH b.name AS name, round(avg(t.fare) * 100) / 100 AS avg_fare "
        "RETURN name, avg_fare ORDER BY avg_fare DESC"
    )
    with g.session() as x:
        o = [{"name": z["name"], "avg_fare": z["avg_fare"]} for z in x.run(q)]
    return jsonify({"companies": o})


@app.get("/area-stats")
def r6():
    a = int(request.args.get("area_id"))
    r = d.filter(col("dropoff_area") == a).groupBy().agg(
        count(lit(1)).alias("trip_count"),
        avg(col("fare")).alias("avg_fare"),
        avg(col("trip_seconds")).alias("avg_trip_seconds"),
    ).collect()
    if not r or r[0]["trip_count"] == 0:
        return jsonify({"area_id": a, "trip_count": 0, "avg_fare": 0.0, "avg_trip_seconds": 0})
    z = r[0]
    return jsonify({
        "area_id": a,
        "trip_count": int(z["trip_count"]),
        "avg_fare": round(float(z["avg_fare"]), 2),
        "avg_trip_seconds": int(round(float(z["avg_trip_seconds"]))),
    })


@app.get("/top-pickup-areas")
def r7():
    n = int(request.args.get("n", 5))
    r = d.groupBy("pickup_area").agg(count(lit(1)).alias("trip_count")).orderBy(desc("trip_count")).limit(n).collect()
    o = [{"pickup_area": int(z["pickup_area"]), "trip_count": int(z["trip_count"])} for z in r]
    return jsonify({"areas": o})


@app.get("/company-compare")
def r8():
    a = request.args.get("company1", "")
    b = request.args.get("company2", "")
    d.createOrReplaceTempView("t2")
    a2 = a.replace("'", "''")
    b2 = b.replace("'", "''")
    q = (
        "SELECT company, "
        "COUNT(*) AS trip_count, "
        "ROUND(AVG(fare), 2) AS avg_fare, "
        "ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute, "
        "ROUND(AVG(trip_seconds), 0) AS avg_trip_seconds "
        f"FROM t2 WHERE company IN ('{a2}', '{b2}') "
        "GROUP BY company"
    )
    r = s.sql(q).collect()
    m = {z["company"] for z in r}
    if a not in m or b not in m:
        return jsonify({"error": "one or more companies not found"})
    o = []
    for z in r:
        o.append({
            "company": z["company"],
            "trip_count": int(z["trip_count"]),
            "avg_fare": float(z["avg_fare"]),
            "avg_fare_per_minute": float(z["avg_fare_per_minute"]),
            "avg_trip_seconds": int(z["avg_trip_seconds"]),
        })
    return jsonify({"comparison": o})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
