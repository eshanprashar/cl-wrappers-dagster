##################################################
# Download CourtListener data with an API token
# and save it to a local directory. As of Sep 2024
# CourtListener restricts the number of requests
# to 5000 per day. This is sufficient unless someone
# wants to scrape large amounts of PACER, opinion data

# The CourtListener API documentation can be found at:
# https://www.courtlistener.com/help/api/rest/v4 # Docs updated to v4 on September 20, 2024


import os
import requests
import logging
import pandas as pd
import json
import csv
import time
from dagster_aws.s3 import S3Resource
from io import BytesIO, StringIO

ROOT_DIR = "cl_wrappers_aws"

class APIScraper:
    def __init__(
        self, base_url, api_token=None, max_requests_per_hour=5000, log_dir="logs",context=None):
        """
        Initialize the APIScraper.

        Args:
            base_url (str): The base URL of the API.
            api_token (str, optional): The API token for authentication. Defaults to None.
            max_requests_per_hour (int, optional): Maximum number of requests allowed per hour. Defaults to 5000.
            log_dir (str, optional): Directory for storing logs. Defaults to "logs".
        """
        self.base_url = base_url
        self.api_token = api_token
        self.max_requests_per_hour = max_requests_per_hour
        self.request_count = 0
        self.log_dir = log_dir
        self.log_file = None  # Initialize the log file path
        self.context = context
        self.session = self.make_session(api_token,context)

        # Create log directory if it does not exist
        os.makedirs(log_dir, exist_ok=True)

    def make_session(self, api_token,context):
        """
        Create and return a session with the required headers, using Dagster's logging.

        Args:
            api_token (str): The API token for authentication.

        Returns:
            requests.Session: A session object with the appropriate headers.
        """
        context.log.info("Creating a new session...")
        session = requests.Session()
        if api_token:
            session.headers.update({"Authorization": f"Token {api_token}"})
        context.log.info("Session created successfully!")
        return session

    def set_data_directory(self, endpoint, context):
        """Creates a data directory inside root directory for the endpoint if it doesn't exist already"""
        
        # Define the data directory inside cl_wrappers_aws
        data_dir = os.path.join(ROOT_DIR, "data", endpoint)

        # Create the data directory if it doesn't exist
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        context.log.info(f"Data directory set to {data_dir}")
        return data_dir

    def create_log_file(self, endpoint, is_author_based=False, author_id=None,context=None):
        """Create a log file based on the endpoint or author ID
        Args:
            endpoint (str): The API endpoint to fetch data from.
            is_author_based (bool, optional): Whether to create a log file per author. Defaults to False.
            author_id (int, optional): The author ID to include in the log file name if is_author_based is True.
        """
        # Determine the log filename based on whether it's author-based
        if is_author_based and author_id:
            log_filename = (
                f"{endpoint}_author_{author_id}_log.txt"  # Log file for each author
            )
        else:
            log_filename = (
                f"{endpoint}_log.txt"  # Default log file for the entire endpoint
            )
        log_path = os.path.join(self.log_dir, log_filename)
        self.log_file = log_path

        # Create the file if it doesn't exist, but don't write anything to it
        if not os.path.exists(log_path):
            with open(log_path, "w") as f:
                pass  # This will create an empty file if it doesn't exist

    def log_progress(self, current_page, context):
        """
        Log the progress of the data fetching process.
        Simple print statement for now. Can be extended to write to a log file.

        Args:
            current_page (int): The current page number.
        """
        context.log.info(f"Request successful! Fetching page {current_page}...")

    def save_current_next_url_and_page(
        self, 
        url, 
        next_url, 
        page_number,
        context):
        """Save the next URL and the last successfully fetched page number to the log file."""
        with open(self.log_file, "w") as f:
            f.write(f"Current URL: {url}\n")
            f.write(f"Next URL: {next_url}\n")
            f.write(f"Last successfully fetched page: {page_number}\n")
        context.log.info(f"Current, next URL and page number saved to {self.log_file}")


    # Function to get the last page number fetched
    def get_last_page(self):
        """Retrieve the last page number fetched from the log file
        Returns:
        int: The last page number fetched
        """
        try:
            with open(self.log_file, "r") as f:
                log_content = f.read().strip()
                # The log file contains "Last successfully fetched page: X"
                last_page_line = [
                    line
                    for line in log_content.split("\n")
                    if "Last successfully fetched page" in line
                ]
                if last_page_line:
                    last_page = int(
                        last_page_line[0].split(":")[-1].strip()
                    )  # Extract page number
                    return last_page
        except:
            return 1  # Default to start from page 1 if no valid log file is found
        return 1  # Default to start from page 1 if no log file is found

    def get_next_url(self):
        """
        Get the next URL from the log file to resume from where we left off.
        """
        try:
            with open(self.log_file, "r") as f:
                log_content = f.read().strip()
                # The log file contains "Next URL: <url>"
                next_url_line = [
                    line for line in log_content.split("\n") if "Next URL:" in line
                ]
                if next_url_line:
                    next_url = next_url_line[0].split("Next URL: ")[-1].strip()
                    return next_url
        except:
            return None  # Default to None if no valid log file is found
        return None  # Default to None if no log file is found

    def fetch_data(
        self, 
        endpoint, 
        params=None, 
        is_author_based=False, 
        save_logic= 'save_after_pages', # Possible options are 'author_level' or 'save_after_pages'
        num_pages_to_save = None, # Required argument if save_logic is 'save_after_pages'
        max_pages = None,
        return_all_data = False,
        storage_type='local', # or 'cloud' depending on setup
        s3_bucket=None,
        s3_key=None,
        context=None):
        """
        Fetch data from the API endpoint with pagination handling.

        Args:
            endpoint (str): The API endpoint to fetch data from.
            params (dict, optional): Additional query parameters for the API request.
            is_author_based (bool, optional): If True, the logic will handle saving data per author. Defaults to False.
            save_logic (str): The logic for saving data. 'author_level' or 'save_after_pages'.
            num_pages_to_save (int, optional): Number of pages after which to save data if save_logic is 'save_after_pages'. 
                                               This is required when save_logic is 'save_after_pages'.
            return_all_data (bool, optional): Whether to return the full dataset after fetching. Defaults to False.
            context: Additional context for log management.
        """

        # Ensure num_pages_to_save is provided when using 'save_after_pages'
        if save_logic == 'save_after_pages' and num_pages_to_save is None:
            error_message = "num_pages_to_save must be provided when save_logic is 'save_after_pages'."
            context.log.error(error_message)
            raise ValueError(error_message)

        # Ensure num_pages_to_save is a valid integer
        num_pages_to_save = num_pages_to_save or 1  # Default to 1 if not provided

        # Check if is_author_based is True
        if is_author_based:
            # Extract author_id from the params['q'] query string
            query = params.get("q", "")
            if "author_id:" in query:
                author_id = query.split("author_id:")[-1]
            else:
                author_id = None  # If no author_id is found in the query
        else:
            author_id = None  # Initialize author_id to None when is_author_based is False

        # Create log file based on endpoint or author
        # This abstracts away the need to modify method signature for each request type (author-based or not)
        self.create_log_file(endpoint, is_author_based, author_id)
        context.log.info(f"Log file created!")

        # Get the next URL to resume from, if available
        next_url = self.get_next_url()  
        url = next_url if next_url else f"{self.base_url}{endpoint}/"

        all_data = []
        # Get the last page number fetched
        last_fetched_page = self.get_last_page()
        context.log.info(f"Last fetched page: {last_fetched_page}")
        if last_fetched_page == 1:
            page_number = last_fetched_page
        else:
            page_number = last_fetched_page - (last_fetched_page % num_pages_to_save)  # Adjust to last saved batch
        total_fetched = 0  # Track the total number of fetched items
        pages_fetched = 0  # Track the number of pages fetched

        # Track time for progress reporting
        start_time = time.time()

        # In case of server side errors, we can delay and retry
        max_retries = 5  # we will loop through these many times
        retry_delay = 5  # seconds

        # Main loop to go through each page, fetch data and save it to a CSV file
        try:
            while url:
                # Break if max_pages is reached
                if max_pages is not None and pages_fetched >= max_pages:
                    context.log.info(f"Max pages limit reached. Exiting...")
                    break
                # Handle rate limit
                if self.request_count >= self.max_requests_per_hour:
                    print(f"API limit reached. Pausing for 1 hour...")
                    context.log.info(f"API limit reached. Pausing for 1 hour...")
                    time.sleep(3600)  # Sleep for 1 hour
                    self.request_count = 0  # Reset the request count after sleeping

                # Retry mechanism
                for attempt in range(max_retries):
                    context.log.info(f"Fetching data from page {page_number}...")
                    response = self.session.get(url, params=params)
                    self.request_count += 1
                    print(f"Request count: {self.request_count}")

                    # Read the response code
                    # Break if the response is successful
                    if response.status_code == 200:
                        break
                    # If the response indicates a server side error, log the error and retry
                    elif response.status_code >= 500:
                        error_message = (
                            f"Server error {response.status_code}: {response.reason}"
                        )
                        context.log.info(error_message)
                        print(error_message)
                        time.sleep(retry_delay)
                        retry_delay *= 2  # exponential backoff
                    else:
                        # Client side errors are not retried
                        error_message = (
                            f"Error {response.status_code}: {response.reason}"
                        )
                        print(error_message)
                        context.log.info(error_message)
                        raise requests.exceptions.HTTPError(error_message)
                else:
                    # If all retries fail, raise an exception
                    error_message = f"Failed to fetch page {page_number} after {max_retries} attempts."
                    context.log.info(error_message)
                    raise requests.exceptions.RetryError(error_message)

                # Reset the retry delay
                retry_delay = 5

                # Read the response data
                data = response.json()
                all_data.extend(data.get("results", []))

                # Update the total number of fetched items
                total_fetched += len(data.get("results", []))
                update_message = f"Total items successfully fetched from page {page_number}: {total_fetched}"
                context.log.info(update_message)

                # Process save logic
                if save_logic == 'save_after_pages' and page_number % num_pages_to_save == 0:
                    # Process and save incrementally
                    self.trigger_save_logic(
                        endpoint,
                        all_data,
                        page_number,
                        num_pages_to_save,
                        context,
                        storage_type=storage_type,
                        s3_bucket=s3_bucket,
                        s3_key=s3_key
                        )
                    all_data = []  # Reset the data after saving

                # Save next URL and page number
                self.save_current_next_url_and_page(
                    url, 
                    data.get("next", "None"), 
                    page_number,
                    context
                )
                # Handle pagination
                if "next" in data and data["next"]:
                    url = data["next"]
                    page_number += 1
                    pages_fetched += 1
                else:
                    break
        except KeyboardInterrupt:
            context.log.info(f"Process interrupted. Last page URL: {url}, Last fetched page: {page_number}, Total records fetched: {total_fetched}")
        finally:
            total_time = time.time() - start_time
            context.log.info(f"Total {total_fetched} records fetched in {total_time/60:.2f} minutes!")
        if return_all_data:
            return all_data
        else:
            return {"status": "success", "message": "Data saved to CSV", "pages_fetched": pages_fetched}

    def trigger_save_logic(
        self, 
        endpoint,
        data,
        page_number,
        num_pages_to_save,
        context,
        storage_type='local',
        s3_bucket=None,
        s3_key=None
        ):
        """
        Save data to CSV based on the page number and save logic.
        
        Args:
            endpoint (str): The API endpoint.
            data (list): The fetched data to save.
            page_number (int): The current page number.
            num_pages_to_save (int): How many pages to save in one file.
        """
        # Process the data
        processed_data = self.process_data(data,context)

        # Define filename using endpoint and page number
        csv_filename = os.path.join(
            self.set_data_directory(endpoint,context),
            f"{endpoint}_pages_{page_number - num_pages_to_save + 1}_to_{page_number}.csv"
        )

        # Save the data to a CSV file
        self.save_to_csv(
            processed_data,
            csv_filename,
            storage_type=storage_type,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            context=context
            )

    # Function to process and flatten the data
    def process_data(self,data,context):
        processed_data = []
        for item in data:
            # Flatten the dictionary and handle missing keys
            entry = {key: item.get(key, None) for key in item}
            processed_data.append(entry)
        processing_message = f"Processed {len(processed_data)} entries"
        context.log.info(processing_message)
        return processed_data

    # Function to save the data to a CSV file
    def save_to_csv(self, data, filename,storage_type='local',s3_bucket=None,s3_key=None,context=None):
        """
        Save the processed data to a CSV file.

        Args:
            data (list): The list of processed data (dictionaries).
            filename (str): The name of the CSV file to save the data.
        """
        if not data:
            error_message = f"No data to save for {filename}"
            context.log.error(error_message)
            print(error_message)
            return

        if storage_type == 'local':
            # Ensure the directory exists
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            # Write to CSV
            with open(filename, "w", newline="") as csvfile:
                fieldnames = data[0].keys()  # Get the column names from the first record
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()  # Write the header
                for item in data:
                    writer.writerow(item)  # Write each row
            success_message = f"Data saved to {filename} in local storage"
            context.log.info(success_message)
            print(success_message)
        elif storage_type == 's3' and s3_bucket and s3_key:
            try:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
                s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())
                success_message = f"Data saved to {s3_key} in S3 bucket {s3_bucket}"
                context.log.info(success_message)
                print(success_message)
            except Exception as e:
                error_message = f"Error saving data to S3: {e}"
                context.log.error(error_message)

class CLScraper(APIScraper):
    """This class is used to scrape data from CourtListener
    Initially, it will serve to scrape positions, education, financial disclosures and docket data
    using CourtListener's search API. We will create two distinct methods to scrape docket data.
    """

    def __init__(self, api_token, context):
        """
        Initialize the CLScraper.

        Args:
            api_token (str): The API token for authentication.
            max_requests_per_hour (int, optional): Maximum number of requests allowed per hour. Defaults to 5000.
        """
        super().__init__(
            base_url="https://www.courtlistener.com/api/rest/v4/",
            api_token=api_token,
            context=context
        )

    def fetch_positions(
        self,
        context,
        is_author_based=False, 
        save_logic= 'save_after_pages', # Possible options are 'author_level' or 'save_after_pages'
        num_pages_to_save = 5,
        max_pages = 20,
        storage_type='local',
        s3_bucket=None, 
        s3_key=None, 
        **kwargs
        ):
        """
        Fetch positions data from the CourtListener API.

        Args:
            **kwargs: Additional query parameters for the API request.

        Returns:
            list: A list of positions data.
        """
        # Define params, default to an empty dictionary if not provided
        params = kwargs.get('params', {})  # Extract 'params' from kwargs, or use an empty dict

        return self.fetch_data(
            endpoint="positions",
            context=context,
            is_author_based=False,
            save_logic= 'save_after_pages',
            num_pages_to_save = num_pages_to_save, # Required argument if save_logic is 'save_after_pages'; save a csv after every 20 pages
            max_pages = max_pages,
            return_all_data = False, # we don't need to assign a variable to the return value
            storage_type=storage_type,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            params=params)

    def fetch_education(self,context,storage_type='local',s3_bucket=None, s3_key=None, **kwargs):
        """
        Fetch education data from the CourtListener API.

        Args:
            **kwargs: Additional query parameters for the API request.

        Returns:
            list: A list of education data.
        """
        # Define params, default to an empty dictionary if not provided
        params = kwargs.get('params', {})  # Extract 'params' from kwargs, or use an empty dict

        return self.fetch_data(
            endpoint="education",
            context=context,
            is_author_based=False,
            save_logic= 'save_after_pages',
            num_pages_to_save = num_pages_to_save, # Required argument if save_logic is 'save_after_pages'; save a csv after every 20 pages
            return_all_data = False, # we don't need to assign a variable to the return value
            storage_type=storage_type,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            params=params)

    def fetch_financial_disclosures(self,context,storage_type='local',s3_bucket=None, s3_key=None, **kwargs):
        """
        Fetch financial disclosures data from the CourtListener API.

        Args:
            **kwargs: Additional query parameters for the API request.

        Returns:
            list: A list of financial disclosures data.
        """
        # Define params, default to an empty dictionary if not provided
        params = kwargs.get('params', {})  # Extract 'params' from kwargs, or use an empty dict

        return self.fetch_data(
            endpoint="financial-disclosures",
            context=context,
            is_author_based=False,
            save_logic= 'save_after_pages',
            num_pages_to_save = num_pages_to_save, # Required argument if save_logic is 'save_after_pages'; save a csv after every 20 pages
            return_all_data = False, # we don't need to assign a variable to the return value
            storage_type=storage_type,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            params=params)

    def extract_judge_name(self,data):
        '''Extract judge name from data'''
        if data:
            judge_name = "name not found"
            for entry in data:
                if entry.get("judge"):
                    judge_name = entry.get("judge")
                    break
        else:
            judge_name = "data not found"
        
        # Sanitizing judge name before assigning to filename
        judge_name = "".join(c if c.isalnum() else "_" for c in judge_name)
        return judge_name
        
    
    def fetch_dockets_per_author_id(self,context,author_id, is_author_based=True,storage_type='local',s3_bucket=None, s3_key=None, **kwargs):
        """
        Fetch dockets data from the CourtListener API.

        Args:
            author_id (int): The author ID for the dockets.
            **kwargs: Additional query parameters for the API request.

        Returns:
            list: A list of dockets data.
        """
        endpoint = "search"
        params = {"q": f"author_id:{author_id}"}
        all_data = self.fetch_data(
            endpoint=endpoint,
            context=context,
            is_author_based=True,
            save_logic= 'author_level',
            return_all_data = True, # we want fetch_data to return the data
            storage_type=storage_type,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            params=params
        )
        # Extract judge name from the data
        judge_name = self.extract_judge_name(all_data)

        # Define the csv filename using judge name and author ID
        csv_filename = os.path.join(
            self.set_data_directory("dockets",context), f"{judge_name}_{author_id}.csv"
        )
        self.save_to_csv(data=self.process_data(all_data), filename=csv_filename)
        success_message = f"Data for author_id {author_id} saved to {csv_filename}"
        context.log.info(success_message)
        print(success_message)

#@resource
#def cl_scraper_resource(init_context):
#    return CLScraper(api_key=init_context.resource_config["api_key"])  