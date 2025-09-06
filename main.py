import os
os.environ['CONNECTOR_NAME'] = 'simple-hardcoded-connectors'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment, upload_data
from assets.ember_electricity.ember_electricity import process_ember_electricity
from assets.big_mac_index.big_mac_index import process_big_mac_index
from assets.freddie_mac.freddie_mac import process_freddie_mac

def main():
    validate_environment()
    
    # Process Ember electricity data
    ember_data = process_ember_electricity()
    upload_data(ember_data, "ember_electricity")
    
    # Process Big Mac Index data
    big_mac_data = process_big_mac_index()
    upload_data(big_mac_data, "big_mac_index")
    
    # Process Freddie Mac House Price Index data
    freddie_mac_data = process_freddie_mac()
    upload_data(freddie_mac_data, "freddie_mac_house_price_index")

if __name__ == "__main__":
    main()