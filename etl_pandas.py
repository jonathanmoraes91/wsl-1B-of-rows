import pandas as pd
from multiprocessing import Pool, cpu_count
from tqdm import tqdm #Using this library to have a progress bar on console

# Determine the number of CPU cores available for multiprocessing
CONCURRENCY = cpu_count()

# Define the total number of rows in the file and the chunk size for processing
total_rows = 1_000_000_000 # Total known rows
chunksize = 100_000_000 # Chunk size
filename = "data/measurements.txt" 

def process_chunk(chunk): # This function will process a single chunk of data.
    aggregated = chunk.groupby('station')['measure'].agg(['min', 'max', 'mean']).reset_index()
    return aggregated


def create_df_with_pandas(filename, total_rows, chunksize=chunksize): #This function will create a Dataframe from a large file by reading and proceesing it in chunks using multiprocessing. 
    total_chunks = total_rows // chunksize +(1 if total_rows % chunksize else 0)
    results = []

    with pd.read_csv(filename, sep=';', header=None, names=['station', 'measure'], chunksize=chunksize) as reader:
        with Pool(CONCURRENCY) as pool:
            async_results = [pool.apply_async(process_chunk, (chunk,)) for chunk in tqdm(reader, total=total_chunks, desc="Processing")]
            for r in tqdm(async_results, desc="Collecting results"):
                results.append(r.get())


     # Concatenate all processed chunks into a single DataFrame
    final_df = pd.concat(results, ignore_index=True) #Combine all chunks into a single DataFrame

    # Further aggregate the concatenated DataFrame
    final_aggregated_df = final_df.groupby('station').agg({
        'min': 'min',
        'max': 'max',
        'mean': 'mean'
    }).reset_index().sort_values('station')

    return final_aggregated_df

if __name__ == "__main__":
    import time

    print("Starting file processing!")
    start_time = time.time()

    df = create_df_with_pandas(filename, total_rows, chunksize) # Create the DataFrame by processing the file

    # Calculate and print the time taken for processing    
    took = time.time() - start_time
    print(df.head())
    print(f"Processing took: {took:.2f} sec")