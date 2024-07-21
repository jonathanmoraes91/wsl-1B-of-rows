import duckdb 
import time

def create_duckdb():
    result = duckdb.sql("""
        SELECT station,
               MIN(measure) AS min_temperature,
               CAST(AVG(measure) AS DECIMAL(3,1)) AS mean_temperature,
               MAX(measure) AS max_temperature
            FROM read_csv("data/measurements.txt", AUTO_DETECT=FALSE, sep=";", columns={'station':VARCHAR, 'measure': 'DECIMAL(3,1)'})
            GROUP BY station
            ORDER BY station
    """)
    
    # Save the result to a Parquet file
    result.write_parquet('data/measurements_summary.parquet')

if __name__ == "__main__":
    import time
    start_time = time.time()
    create_duckdb()
    took = time.time() - start_time
    print(f"Duckdb Took: {took:.2f} sec")