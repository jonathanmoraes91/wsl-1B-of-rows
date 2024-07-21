import streamlit as st
import duckdb
import pandas as pd

def load_data(): # Function to load data from Parquet file
    con = duckdb.connect()
    df = con.execute("SELECT * FROM 'data/measurements_summary.parquet'").df()
    con.close()
    return df

def main(): #Function to create the dashboard
    st.title("Weather Station Summary")
    st.write("This dashboard shows the summary of weather station including minimum, mean and maximum temperatures.")

    data = load_data()

    st.dataframe(data)

if __name__ == "__main__":
    main()