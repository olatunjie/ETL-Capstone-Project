import configparser
import json

from utils.helper import read_commodity_data
from utils.helper import read_other_data
# from utils.helper import extract_api_data
from utils.helper import create_bucket
from utils.helper import Save_data_to_bucket
from utils.helper import create_dwh_dev
from utils.helper import create_local_dev_tables,transfer_data,copy_data_to_redshift,create_dwh_star,copy_data_to_dwh



# Extract data for each category from Bloomberg API and save as a JSON file
#extract_api_data()


# Create the tables on local db
create_local_dev_tables()

# Read the JSON files and Save in Postgres DB
read_commodity_data()
read_other_data()

#Create s3 Bucket (Data Lake)
create_bucket()

# Save Data ascsv in bucket
Save_data_to_bucket()

# Copy Data to Dev Environment
create_dwh_dev()
copy_data_to_redshift()

# Copy Data to Prod Environment
create_dwh_star()

# Move Data to Datawarehouse
copy_data_to_dwh()
