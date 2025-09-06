"""Fetch and process Freddie Mac House Price Index data"""
import pyarrow as pa
from utils.http_client import get
from utils.io import load_state, save_state
from datetime import datetime, timezone

DATA_SOURCE_URL = 'https://www.freddiemac.com/fmac-resources/research/docs/fmhpi_master_file.csv'

def fetch_freddie_mac_data():
    """Fetch Freddie Mac House Price Index data"""
    response = get(DATA_SOURCE_URL)
    response.raise_for_status()
    return response.text

def process_freddie_mac():
    """Process Freddie Mac House Price Index data"""
    import pandas as pd
    from io import StringIO
    
    # Load state
    state = load_state("freddie_mac")
    
    # Fetch data
    csv_data = fetch_freddie_mac_data()
    df = pd.read_csv(StringIO(csv_data), low_memory=False)
    
    # Create date column from Year and Month
    df['date'] = pd.to_datetime(df['Year'].astype(str) + '-' + df['Month'].astype(str).str.zfill(2) + '-01')
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    # Rename columns to lowercase with underscores
    df = df.rename(columns={
        'GEO_Type': 'geo_type',
        'GEO_Code': 'geo_code', 
        'GEO_Name': 'geo_name',
        'Year': 'year',
        'Month': 'month',
        'Index_NSA': 'index_nsa',
        'Index_SA': 'index_sa'
    })
    
    # Convert geo_code to string and handle missing values
    df['geo_code'] = df['geo_code'].astype(str)
    df['geo_code'] = df['geo_code'].replace('.', None)
    
    # Select relevant columns
    df = df[['date', 'year', 'month', 'geo_type', 'geo_code', 'geo_name', 'index_nsa', 'index_sa']]
    
    # Convert to PyArrow table
    table = pa.Table.from_pandas(df, preserve_index=False)
    
    # Update state
    save_state("freddie_mac", {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "row_count": len(table)
    })
    
    return table