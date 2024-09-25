from dagster import asset, EnvVar, OpExecutionContext
from dagster_aws.s3 import S3Resource
from cl_wrappers_aws.resources.api_scraper import APIScraper, CLScraper

api_token = EnvVar("API_TOKEN")

@asset
def create_CLscraper_object(context: OpExecutionContext) -> CLScraper:
    cl_scraper = CLScraper(api_token=api_token)
    context.log.info('CLScraper object created successfully.')
    return cl_scraper

@asset
def position_csv_files(context, create_CLscraper_object: CLScraper, s3: S3Resource) -> None:
    # Asset logic here
    context.log.info("Starting position data extraction...")
    create_CLscraper_object.fetch_positions(
        context=context,
        storage_type='s3',  # or 'local' depending on your setup
        s3_bucket='your-s3-bucket-name',
        s3_key='positions/',
        # Add any additional parameters if needed
    )
    context.log.info("Position data extraction completed.")

#@asset
#def financial_disclosures_csv_files():




#@asset
#def dockets_per_judge_csv_files():


