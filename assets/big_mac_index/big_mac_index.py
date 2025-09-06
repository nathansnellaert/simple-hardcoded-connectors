import pyarrow as pa
import pyarrow.csv as csv
from utils import get, load_state, save_state
from datetime import datetime
import io

def process_big_mac_index():
    """Process Big Mac Index data from The Economist GitHub repository."""
    
    state = load_state("big_mac_index")
    
    url = "https://raw.githubusercontent.com/TheEconomist/big-mac-data/master/output-data/big-mac-full-index.csv"
    response = get(url)
    response.raise_for_status()
    
    table = csv.read_csv(io.BytesIO(response.content))
    
    # Select and rename relevant columns
    column_mapping = {
        'iso_a3': 'country_code',
        'currency_code': 'currency_code',
        'name': 'country_name',
        'date': 'date',
        'local_price': 'local_price',
        'dollar_ex': 'exchange_rate',
        'dollar_price': 'dollar_price',
        'USD_raw': 'usd_raw_index',
        'EUR_raw': 'eur_raw_index',
        'GBP_raw': 'gbp_raw_index',
        'JPY_raw': 'jpy_raw_index',
        'CNY_raw': 'cny_raw_index',
        'GDP_dollar': 'gdp_per_capita',
        'adj_price': 'adjusted_price',
        'USD_adjusted': 'usd_adjusted_index',
        'EUR_adjusted': 'eur_adjusted_index',
        'GBP_adjusted': 'gbp_adjusted_index',
        'JPY_adjusted': 'jpy_adjusted_index',
        'CNY_adjusted': 'cny_adjusted_index'
    }
    
    # Select only columns we have mappings for
    available_columns = [col for col in column_mapping.keys() if col in table.column_names]
    table = table.select(available_columns)
    
    # Rename columns
    new_names = [column_mapping[col] for col in available_columns]
    table = table.rename_columns(new_names)
    
    # Drop rows with missing essential values
    import pyarrow.compute as pc
    essential_columns = ['country_code', 'date', 'dollar_price']
    for col in essential_columns:
        if col in table.column_names:
            table = table.filter(pc.is_valid(table[col]))
    
    save_state("big_mac_index", {
        'last_updated': datetime.now().isoformat(),
        'row_count': len(table)
    })
    
    return table