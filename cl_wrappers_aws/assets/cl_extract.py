from dagster import asset, EnvVar, OpExecutionContext
from dagster_aws.s3 import S3Resource
from cl_wrappers_aws.resources.api_scraper import APIScraper, CLScraper

# Load environment variables
api_token_var = EnvVar("API_TOKEN")

@asset
def position_csv_files(context:OpExecutionContext, s3: S3Resource) -> dict:
    # Retrieve the api token
    api_token = api_token_var.get_value()

    # Create the CLScraper object
    cl_scraper = CLScraper(api_token=api_token,context=context)
    
    # Define storage type 
    storage_type = 'local'

    # Log start of the process
    context.log.info("Starting position data extraction...")
    cl_scraper.fetch_positions(
        context=context,
        is_author_based=False, 
        save_logic= 'save_after_pages', # Possible options are 'author_level' or 'save_after_pages'
        num_pages_to_save = 5,
        max_pages=20,
        storage_type=storage_type,  # or 'local' depending on your setup
        s3_bucket=None,
        s3_key=None,
        # Add any additional parameters if needed
    )
    context.log.info("Position data extraction completed.")
    # Return metadata: temporary fix to handle dependency in clean_positions_dataframe
    return {
        "storage_type": storage_type
    }


@asset
def financial_disclosures_csv_files(context:OpExecutionContext, s3: S3Resource) -> None:
    # Retrieve the api token
    api_token = api_token_var.get_value()

    # Create the CLScraper object
    cl_scraper = CLScraper(api_token=api_token,context=context)
    
    # Log start of the process
    context.log.info("Starting financial disclosures data extraction...")
    cl_scraper.fetch_disclosures(
        context=context,
        is_author_based=False, 
        save_logic= 'save_after_pages', # Possible options are 'author_level' or 'save_after_pages'
        num_pages_to_save = 5,
        max_pages=20,
        storage_type='local',  # or 'local' depending on your setup
        s3_bucket=None,
        s3_key=None,
        # Add any additional parameters if needed
    )
    context.log.info("Financial disclosures data extraction completed.")


#@asset
#def dockets_per_judge_csv_files():


