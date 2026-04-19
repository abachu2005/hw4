import pandas as pd

a = pd.read_csv("taxi_trips.csv")

b = ["Trip ID", "Taxi ID", "Company", "Pickup Community Area",
     "Dropoff Community Area", "Fare", "Trip Seconds"]
a = a[b]

a.columns = ["trip_id", "driver_id", "company", "pickup_area",
             "dropoff_area", "fare", "trip_seconds"]

a = a.dropna(subset=["trip_id", "driver_id", "company",
                     "pickup_area", "dropoff_area", "fare", "trip_seconds"])

a["pickup_area"] = a["pickup_area"].astype(int)
a["dropoff_area"] = a["dropoff_area"].astype(int)
a["fare"] = a["fare"].astype(float)
a["trip_seconds"] = a["trip_seconds"].astype(int)

a = a[a["fare"] > 0]
a = a[a["trip_seconds"] > 0]

a = a.head(10000)

a.to_csv("taxi_trips_clean.csv", index=False)
print(f"Cleaned dataset: {len(a)} rows")
