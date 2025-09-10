import os

# Set environment variables for this run
os.environ['CONNECTOR_NAME'] = 'simple-hardcoded-connectors'
os.environ['RUN_ID'] = 'local-dev'
os.environ['ENABLE_HTTP_CACHE'] = 'true'
os.environ['CATALOG_TYPE'] = 'local'
os.environ['DATA_DIR'] = 'data'

from main import main

# Run the connector
main()

print("\nConnector completed successfully!")