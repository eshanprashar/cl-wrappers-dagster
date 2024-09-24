##################################################
# Download CourtListener data with an API token
# and save it to a local directory. As of Sep 2024
# CourtListener restricts the number of requests
# to 5000 per day. This is sufficient unless someone
# wants to scrape large amounts of PACER, opinion data

# The CourtListener API documentation can be found at:
# https://www.courtlistener.com/help/api/rest/v4 # Docs updated to v4 on September 20, 2024


import os
from dotenv import load_dotenv
import requests
import logging
import pandas as pd
import json
import csv
import time
from utils import get_api_token, log_error_to_file


class APIScraper:
    def __init__(
        self, base_url, api_token=None, max_requests_per_hour=5000, log_dir="logs"
    ):
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
        self.session = self.make_session(api_token)
        self.log_dir = log_dir
        self.log_file = None  # Initialize the log file path

        # Create log directory if it does not exist
        os.makedirs(log_dir, exist_ok=True)

    @log_error_to_file(log_dir="logs")
    def make_session(self, api_token):
        """
        Create and return a session with the required headers.

        Args:
            api_token (str): The API token for authentication.

        Returns:
            requests.Session: A session object with the appropriate headers.
        """
        session = requests.Session()
        if api_token:
            session.headers.update({"Authorization": f"Token {api_token}"})
        return session

    def set_data_directory(self, endpoint):
        """Creates a data directory for the endpoint if it doesn't exist already"""
        # Get the current working directory
        current_dir = os.getcwd()
        # Traverse up to find the root directory
        root_dir = os.path.dirname(
            current_dir
        )  # in our case, this should take us to 'scripts'
        if "scripts" in root_dir:
            # Go up one more level
            root_dir = os.path.dirname(
                root_dir
            )  # this should take us to the root directory
        # Define the data directory relative to root
        data_dir = os.path.join(root_dir, "data", endpoint)

        # Create the data directory if it doesn't exist
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        print(f"Data directory set to {data_dir}")
        return data_dir

    def create_log_file(self, endpoint, is_author_based=False, author_id=None):
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
        print(f"Log file created at {log_path}")

    def log_progress(self, current_page):
        """
        Log the progress of the data fetching process.
        Simple print statement for now. Can be extended to write to a log file.

        Args:
            current_page (int): The current page number.
        """
        print(f"Request successful with code 200! Now, fetching page {current_page}...")

    def save_current_next_url_and_page(
        self, url, next_url, page_number, is_author_based=False, author_id=None
    ):
        """Save the next URL and the last successfully fetched page number to the log file."""
        with open(self.log_file, "w") as f:
            f.write(f"Current URL: {url}\n")
            f.write(f"Next URL: {next_url}\n")
            f.write(f"Last successfully fetched page: {page_number}\n")
        print(f"Next URL and page number saved to {self.log_file}")

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

    @log_error_to_file(log_dir="logs")
    def fetch_data(
        self, endpoint, params=None, save_after_pages=10, is_author_based=False
    ):
        """
        Fetch data from the API endpoint with pagination handling.

        Args:
            endpoint (str): The API endpoint to fetch data from.
            params (dict, optional): Additional query parameters for the API request. Defaults to None.
            save_after_pages (int, optional): Save data to a file after every `save_after_pages` pages. Defaults to 10 for testing.
            is_author_based (bool, optional): If True, use a different log file per author. Defaults to False.
            author_id (int, optional): If is_author_based is True, this should contain the author ID for creating a unique log file.

        Returns:
            list: A list of data fetched from the API endpoint.
        """
        # Check if is_author_based is True
        if is_author_based:
            # Extract author_id from the params['q'] query string
            query = params.get("q", "")
        if "author_id:" in query:
            author_id = query.split("author_id:")[-1]
        else:
            author_id = None

        # Create log file based on endpoint or author
        # This abstracts away the need to modify method signature for each request type (author-based or not)
        self.create_log_file(endpoint, is_author_based, author_id)

        # Get the next URL to resume from, if available
        next_url = self.get_next_url()  # NEEDS TO BE CHANGED
        url = next_url if next_url else f"{self.base_url}{endpoint}/"

        all_data = []
        # Get the last page number fetched
        last_fetched_page = self.get_last_page()  # NEEDS TO BE CHANGED
        if last_fetched_page == 1:
            page_number = last_fetched_page
        else:
            page_number = last_fetched_page - (
                last_fetched_page % save_after_pages
            )  # Adjust to last saved batch
        total_fetched = 0  # Track the total number of fetched items

        # Track time for progress reporting
        start_time = time.time()

        # In case of server side errors, we can delay and retry
        max_retries = 5  # we will loop through these many times
        retry_delay = 5  # seconds

        # Main loop to go through each page, fetch data and save it to a CSV file
        try:
            while url:
                # Handle rate limit
                if self.request_count >= self.max_requests_per_hour:
                    print(f"API limit reached. Pausing for 1 hour...")
                    time.sleep(3600)  # Sleep for 1 hour
                    self.request_count = 0  # Reset the request count after sleeping

                # Retry mechanism
                for attempt in range(max_retries):
                    print(f"Fetching data from page {page_number}...")
                    response = self.session.get(url, params=params)
                    self.request_count += 1
                    print(f"Request count: {self.request_count}")

                    # Read the response code
                    # Break if the response is successful
                    if response.status_code == 200:
                        break
                    # If the response indicates a server side error, log the error and retry
                    elif response.status_code >= 500:
                        print(
                            f"Server error {response.status_code}: {response.reason}. Retrying in {retry_delay} seconds..."
                        )
                        time.sleep(retry_delay)
                        retry_delay *= 2  # exponential backoff
                    else:
                        # Client side errors are not retried
                        error_message = (
                            f"Error {response.status_code}: {response.reason}"
                        )
                        print(error_message)
                        raise requests.exceptions.HTTPError(error_message)
                else:
                    # If all retries fail, raise an exception
                    error_message = f"Failed to fetch page {page_number} after {max_retries} attempts."
                    print(error_message)
                    raise requests.exceptions.RetryError(error_message)

                # Reset the retry delay
                retry_delay = 5

                # Read the response data
                data = response.json()
                all_data.extend(data.get("results", []))

                # Update the total number of fetched items
                total_fetched += len(data.get("results", []))

                # Print progress
                print(
                    f"Total items successfully fetched from page {page_number}: {total_fetched}"
                )

                # Save data incrementally to csv
                if not is_author_based and page_number % save_after_pages == 0:
                    # Process the data
                    all_data = self.process_data(all_data)
                    # Define filename using endpoint and page number
                    csv_filename = os.path.join(
                        self.set_data_directory(endpoint),
                        f"{endpoint}_page_{page_number}.csv",
                    )
                    # Save the data to a CSV file
                    self.save_to_csv(data=all_data, filename=csv_filename)
                    print(f"Data saved to {csv_filename}")
                    all_data = []  # Reset the data after saving to avoid memory issues

                # Save next URL and page number
                self.save_current_next_url_and_page(
                    url, data.get("next", "None"), page_number
                )

                # Handle pagination
                if "next" in data and data["next"]:
                    url = data["next"]
                    page_number += 1
                else:
                    break
        except KeyboardInterrupt:
            print("\nProcess interrupted. Here's a summary:")
            print(f"Last page URL: {url}")
            print(f"Last fetched page: {page_number}")
            print(f"Total records fetched: {total_fetched}")
            print(
                "Don't worry, the progress has been saved. You can resume from the last page."
            )

        finally:
            print("Exiting gracefully. The last page and URL were logged.")
        total_time = time.time() - start_time
        print(f"Total {total_fetched} records fetched in {total_time/60:.2f} minutes!")
        return all_data

    # Function to process and flatten the data
    def process_data(self, data):
        processed_data = []
        for item in data:
            # Flatten the dictionary and handle missing keys
            entry = {key: item.get(key, None) for key in item}
            processed_data.append(entry)
        print(f"Total number of entries processed: {len(processed_data)}")
        return processed_data

    # Function to save the data to a CSV file
    def save_to_csv(self, data, filename):
        """
        Save the processed data to a CSV file.

        Args:
            data (list): The list of processed data (dictionaries).
            filename (str): The name of the CSV file to save the data.
        """
        if not data:
            print(f"No data to save for {filename}")
            return

        # Ensure the directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        # Write to CSV
        with open(filename, "w", newline="") as csvfile:
            fieldnames = data[0].keys()  # Get the column names from the first record
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()  # Write the header
            for item in data:
                writer.writerow(item)  # Write each row
        print(f"Data saved to {filename}")


class CLScraper(APIScraper):
    """This class is used to scrape data from CourtListener
    Initially, it will serve to scrape positions, education, financial disclosures and docket data
    using CourtListener's search API. We will create two distinct methods to scrape docket data.
    """

    def __init__(self, api_token, max_requests_per_hour=5000):
        """
        Initialize the CLScraper.

        Args:
            api_token (str): The API token for authentication.
            max_requests_per_hour (int, optional): Maximum number of requests allowed per hour. Defaults to 5000.
        """
        super().__init__(
            base_url="https://www.courtlistener.com/api/rest/v4/",
            api_token=api_token,
            max_requests_per_hour=max_requests_per_hour,
        )

    def fetch_positions(self, **kwargs):
        """
        Fetch positions data from the CourtListener API.

        Args:
            **kwargs: Additional query parameters for the API request.

        Returns:
            list: A list of positions data.
        """
        return self.fetch_data(endpoint="positions", params=kwargs)

    def fetch_education(self, **kwargs):
        """
        Fetch education data from the CourtListener API.

        Args:
            **kwargs: Additional query parameters for the API request.

        Returns:
            list: A list of education data.
        """
        return self.fetch_data(endpoint="education", params=kwargs)

    def fetch_financial_disclosures(self, **kwargs):
        """
        Fetch financial disclosures data from the CourtListener API.

        Args:
            **kwargs: Additional query parameters for the API request.

        Returns:
            list: A list of financial disclosures data.
        """
        return self.fetch_data(endpoint="financial-disclosures", params=kwargs)

    def fetch_dockets_per_author_id(self, author_id, is_author_based=True, **kwargs):
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
            endpoint=endpoint, params=params, is_author_based=True
        )

        # If data is not empty, extract name and save to a CSV file
        if all_data:
            judge_name = "name not found"
            for entry in all_data:
                if entry.get("judge"):
                    judge_name = entry.get("judge")
                    break
        else:
            judge_name = "data not found"

        # Sanitizing judge name before assigning to filename
        judge_name = "".join(c if c.isalnum() else "_" for c in judge_name)

        # Define the csv filename using judge name and author ID
        csv_filename = os.path.join(
            self.set_data_directory("dockets"), f"{judge_name}_{author_id}.csv"
        )
        self.save_to_csv(data=self.process_data(all_data), filename=csv_filename)
        print(f"Data for author_id {author_id} saved to {csv_filename}")
        return all_data

@resource
def cl_scraper_resource(init_context):
    return CLScraper(api_key=init_context.resource_config["api_key"])  