import os
import pandas as pd
from neo4j import GraphDatabase

u = os.environ.get("N_URI", "bolt://localhost:7687")
n = os.environ.get("N_USER", "neo4j")
p = os.environ.get("N_PWD", "hw4password")
f = os.environ.get("CSV", "taxi_trips_clean.csv")

d = pd.read_csv(f)
r = d.to_dict(orient="records")

g = GraphDatabase.driver(u, auth=(n, p))

with g.session() as s:
    s.run("MATCH (x) DETACH DELETE x")
    s.run("CREATE INDEX driver_id IF NOT EXISTS FOR (x:Driver) ON (x.driver_id)")
    s.run("CREATE INDEX company_name IF NOT EXISTS FOR (x:Company) ON (x.name)")
    s.run("CREATE INDEX area_id IF NOT EXISTS FOR (x:Area) ON (x.area_id)")

    q = (
        "UNWIND $b AS r "
        "MERGE (a:Driver {driver_id: r.driver_id}) "
        "MERGE (c:Company {name: r.company}) "
        "MERGE (e:Area {area_id: toInteger(r.dropoff_area)}) "
        "MERGE (a)-[:WORKS_FOR]->(c) "
        "CREATE (a)-[:TRIP {trip_id: r.trip_id, fare: toFloat(r.fare), trip_seconds: toInteger(r.trip_seconds)}]->(e)"
    )

    k = 1000
    for i in range(0, len(r), k):
        s.run(q, b=r[i:i + k])

    h = s.run("MATCH (a:Driver) RETURN count(a) AS x").single()["x"]
    j = s.run("MATCH (c:Company) RETURN count(c) AS x").single()["x"]
    m = s.run("MATCH (e:Area) RETURN count(e) AS x").single()["x"]
    t = s.run("MATCH ()-[x:TRIP]->() RETURN count(x) AS x").single()["x"]
    print({"driver_count": h, "company_count": j, "area_count": m, "trip_count": t})

g.close()
