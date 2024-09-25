from dagster import EnvVar
from .api_scraper import CLScraper, APIScraper

# #EnvVar defers resolution of the environment variable value until run time, 
# and should only be used as input to Dagster config or resources.
api_token = EnvVar("API_TOKEN")