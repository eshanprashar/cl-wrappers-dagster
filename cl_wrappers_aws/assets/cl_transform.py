from dagster import asset, EnvVar, OpExecutionContext
from dagster_aws.s3 import S3Resource

# Import necessary libraries for visualization and EDA
import os
import pandas as pd
import matplotlib.pyplot as plt
import glob

ROOT_DIR = "cl_wrappers_aws"

@asset
def clean_positions_dataframe(
    context: OpExecutionContext, 
    position_csv_files: dict,  # Adding dependency for positions metadata
    s3: S3Resource = None # Even passing this as None saves the result in S3 storage
) -> pd.DataFrame:
    
    # Check storage type from metadata
    # Extract storage type and data directory from the dictionary
    storage_type = position_csv_files.get("storage_type", "local")  # Default to 'local' if not specified

    # Load the data
    if storage_type == 's3':
        context.log.info("Loading data from S3 is not implemented yet.")
        pass  # Future logic for loading from S3
    else:
        # Load data from local storage
        context.log.info("Loading data from local storage...")
        data_dir = os.path.join(ROOT_DIR, "data", "positions")
        files = glob.glob(data_dir + "/*.csv")
    
    li = []
    for filename in files:
        try:
            context.log.info(f"Reading file {filename}...")
            df = pd.read_csv(filename, index_col=None, header=0)
            li.append(df)
        except Exception as e:
            context.log.error(f"Error reading file {filename}: {e}")
    
    # Concatenate all dataframes into single dataframe
    df = pd.concat(li, axis=0, ignore_index=True)

    # Transformations
    df['position'] = df['position_type'].fillna('') + ' ' + df['job_title'].fillna('')
    df['position'] = df['position'].str.lower().str.strip()
    
    # Add judge flag based on position
    df['judge_flag'] = df['position'].str.contains('jud|jus|mag', na=False).astype(int)

    # Log the unique positions
    context.log.info(f"Total unique positions: {df['position'].nunique()}")
    context.log.info(f"Total unique positions with 'jus', 'jud', or 'mag': {df[df['position'].str.contains('jud|jus|mag', na=False)]['position'].nunique()}")

    # For now, the dataframe will be saved in local; we can think of saving it in S3 later
    if storage_type == 's3':
        context.log.info("Saving to S3 is not implemented yet.")
        pass  # Future logic for saving to S3
    else:
        # Save the dataframe in local storage
        context.log.info("Saving the dataframe in local storage...")
        desired_path = os.path.join(ROOT_DIR, "data", "intermediate_dfs")
        if not os.path.exists(desired_path):
            os.makedirs(desired_path)
        # Save the dataframe
        df.to_csv(os.path.join(desired_path, "cleaned_positions.csv"), index=False)
        context.log.info(f"Dataframe with size {df.size} cleaned and saved successfully.")

    return df


#@asset
#def positions_summary_stats():




#@asset
#def positions_judge_ids():



#@asset
#def cleaned_disclosures_df():



#@asset
#def disclosures_stats():




#@asset
#def judges_with_disclosures_ids():



#@asset
#def 
