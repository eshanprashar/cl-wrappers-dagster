from dagster import asset, AssetExecutionContext
from .resources import api_token
from dagster_aws.s3 import S3Resource


@asset
def create_CLscraper_object(context)->CLScraper:
    cl_scrapper = CLScraper()
    context.log.info('API Scraper initialized...')
    return cl_scrapper

@asset(
    deps=['create_CLscraper_object']
)
def position_csv_files(context, create_CLscraper_object: CLScraper, s3: S3Resource) -> None:
    create_CLscraper_object.fetch_positions(
        context=context,
        storage_type='s3',  # or 'local'
        s3_bucket='your-s3-bucket-name',
        s3_key='positions/',
        **kwargs
    )

@asset
def financial_disclosures_csv_files():




@asset
def dockets_per_judge_csv_files():


