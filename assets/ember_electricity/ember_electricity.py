import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from utils import get

def process_ember_electricity() -> pa.Table:
    """
    Fetch and process Ember Climate yearly electricity data.
    
    This dataset contains yearly electricity generation, capacity, emissions, 
    import and demand data for over 200 geographies. Updated twice monthly 
    with data through 2024.
    
    Returns:
        PyArrow table containing the cleaned electricity data
    """
    url = "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/yearly_full_release_long_format.csv"
    
    print("Fetching Ember electricity data...")
    response = get(url, timeout=300.0)
    response.raise_for_status()
    
    # Parse CSV directly to PyArrow table
    table = csv.read_csv(pa.py_buffer(response.content))
    
    # Select and rename columns
    columns_to_keep = {
        'Area': 'country_name',
        'ISO 3 code': 'country_iso3_code', 
        'Year': 'year',
        'Area type': 'area_type',
        'Continent': 'continent',
        'Category': 'category',
        'Subcategory': 'subcategory',
        'Variable': 'variable',
        'Unit': 'unit',
        'Value': 'value',
        'YoY absolute change': 'yoy_absolute_change',
        'YoY % change': 'yoy_percent_change'
    }
    
    # Select only the columns we want
    selected_columns = []
    new_names = []
    for old_name, new_name in columns_to_keep.items():
        if old_name in table.column_names:
            selected_columns.append(table.column(old_name))
            new_names.append(new_name)
    
    # Create new table with selected and renamed columns
    table = pa.Table.from_arrays(selected_columns, names=new_names)
    
    # Get some stats about the data
    if 'year' in table.column_names:
        year_column = table.column('year')
        latest_year = pc.max(year_column).as_py()
        earliest_year = pc.min(year_column).as_py()
        print(f"Data covers years {earliest_year} to {latest_year}")
    
    print(f"Processed Ember electricity data: {table.num_rows:,} rows x {table.num_columns} columns")
    print(f"Columns: {', '.join(table.column_names)}")
    
    return table