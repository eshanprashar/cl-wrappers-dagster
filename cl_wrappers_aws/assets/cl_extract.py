from dagster import asset
from resources.api_scraper import APIScraper, CLScraper
from dagster_aws.s3 import S3Resource


@asset(required_resource_keys={"cl_scraper_resource"})
def create_CLscraper_object()->CLScraper:
    cl_scrapper = CLScraper(api_token="some token")
    return cl_scrapper

@asset
def position_csv_files():



@asset
def financial_disclosures_csv_files():




@asset
def dockets_per_judge_csv_files():


