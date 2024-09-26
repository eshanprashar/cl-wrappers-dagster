from dagster import asset, EnvVar, OpExecutionContext
from dagster_aws.s3 import S3Resource

# Import necessary libraries for visualization and EDA
import os
import pandas as pd
import matplotlib.pyplot as plt
import glob
import json
import ast

ROOT_DIR = "cl_wrappers_aws"


@asset
def consolidated_position_and_title(
    context: OpExecutionContext, 
    position_csv_files: dict,  # Adding dependency for positions metadata
    s3: S3Resource = None # Even passing this as None saves the result in S3 storage
    ) -> None:
    '''
    This function reads the position data, cleans it, and saves it back as a csv.
    '''
    
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
        df.to_csv(os.path.join(desired_path, "consolidated_positions.csv"), index=False)
        context.log.info(f"Dataframe has {df.shape[0]} rows and saved successfully.")

@asset(
    deps=["consolidated_position_and_title"]
)
def positions_data_with_persons_info(
    context: OpExecutionContext, 
    s3: S3Resource = None
    ) -> None:
    '''
    This function extracts person information from the positions data and saves a new csv
    '''
    # Load the consolidated positions dataframe
    file_name = "consolidated_positions.csv"
    data_path = os.path.join(ROOT_DIR, "data", "intermediate_dfs", file_name)
    df = pd.read_csv(data_path)

    # Extract person information from the 'person' column
    # Check if the 'person' column is a string and try to convert it back to a dictionary
    df['person'] = df['person'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
    
    # Now you can safely access the 'id' field
    df['person_id'] = df['person'].apply(lambda x: x.get('id') if pd.notnull(x) else None)
    df['full_name'] = df['person'].apply(lambda x: x.get('slug') if pd.notnull(x) else None)
    df['name_first'] = df['person'].apply(lambda x: x.get('name_first') if pd.notnull(x) else None)
    df['name_middle'] = df['person'].apply(lambda x: x.get('name_middle') if pd.notnull(x) else None)
    df['name_last'] = df['person'].apply(lambda x: x.get('name_last') if pd.notnull(x) else None)
    df['name_suffix'] = df['person'].apply(lambda x: x.get('name_suffix') if pd.notnull(x) else None)
    df['gender'] = df['person'].apply(lambda x: x.get('gender') if pd.notnull(x) else None)
    df['race'] = df['person'].apply(lambda x: x.get('race') if pd.notnull(x) else None)
    df['religion'] = df['person'].apply(lambda x: x.get('religion') if pd.notnull(x) else None)

    # Save the new dataframe with person info
    output_file = os.path.join(ROOT_DIR, "data", "intermediate_dfs", "positions_with_person_info.csv")
    df.to_csv(output_file, index=False)

    context.log.info(f"Processed data saved to {output_file}")

'''
@asset
def positions_and_persons_data_with_judge_flag(
    context: OpExecutionContext, 
    positions_data_with_persons_info: pd.DataFrame,
    s3: S3Resource = None
    ) -> pd.DataFrame:
    df = positions_data_with_persons_info
    # Create the 'position' column by combining 'position_type' and 'job_title'
    df['position'] = df['position_type'].fillna('') + ' ' + df['job_title'].fillna('')
    df['position'] = df['position'].str.lower().str.strip()

    context.log.info(f"Consolidated position columns into a single 'position' column.")

    # Add 'judge_flag' based on the 'position' column
    df['judge_flag'] = df['position'].str.contains('jud|jus|mag', na=False).astype(int)

    context.log.info(f"Added 'judge_flag' column based on the position titles.")
    # Log the unique positions
    context.log.info(f"Total unique positions: {df['position'].nunique()}")
    context.log.info(f"Total unique positions with 'judge_flag' = 1: {df[df['judge_flag'] == 1]['position'].nunique()}")
    
    # Save the dataframe in local storage
    desired_path = os.path.join(ROOT_DIR, "data", "intermediate_dfs")
    # Save the dataframe
    df.to_csv(os.path.join(desired_path, "positions_with_judge_flag.csv"), index=False)
    return df


@asset
def positions_summary_stats(context, positions_and_persons_data_with_judge_flag: pd.DataFrame) -> None:

    df = positions_and_persons_data_with_judge_flag

    # Let's find the average number of positions (rows) per person_id
    avg_positions = df.groupby('person_id').size().mean()
    context.log.info(f"Average number of positions per person_id is {avg_positions.round(2)}")

    # Let's find the median number of positions per person_id
    median_positions = df.groupby('person_id').size().median()
    context.log.info(f"Median number of positions per person_id is {median_positions}")

    # Now, let's find count of person_ids that ONLY have judge flag = 1. These people have no other positions in the data
    judges_venn = df.groupby('person_id')['judge_flag'].sum()

    # Check if the sum of judge_flag for each person_id is equal to the number of positions they have (indicating all positions are judge-related)
    only_judges_count = (judges_venn == df.groupby('person_id').size()).sum()

    # Now, let's find count of person_ids that ONLY have judge flag = 0. These people are NOT JUDGES
    only_non_judges_count = (judges_venn == 0).sum()

    # Now, let's find count of person_ids that have judge flag = 1 as well as 0. These are judges with other positions
    judges_with_other_positions_count = ((judges_venn > 0) & (judges_venn < df.groupby('person_id').size())).sum()

    # Log the statistics
    context.log.info(f"Total number of person_ids: {df['person_id'].nunique()}")
    context.log.info(f"Count of person_ids that ONLY have judge flag = 1: {only_judges_count}")
    context.log.info(f"Count of person_ids that ONLY have judge flag = 0: {only_non_judges_count}")
    context.log.info(f"Count of person_ids that have judge flag = 1 as well as 0: {judges_with_other_positions_count}")

    
    # Plot the distribution of rows (positions) for each person_id
    plt.figure(figsize=(8,6))
    df.groupby('person_id').size().plot(kind='hist', bins=100)
    plt.xlabel('Number of positions')
    plt.ylabel('Number of person_ids')
    plt.title('Distribution of positions per person_id')

    # Set Y-axis ticks to intervals of 500
    max_y = plt.gca().get_ylim()[1]
    plt.yticks(range(0, int(max_y) + 1, 500))

    # Plot the average value as a vertical line
    plt.axvline(avg_positions, color='r', linestyle='dashed', linewidth=2)
    plt.text(avg_positions, max_y * 0.95, f'Avg: {avg_positions:.2f}', color='r', ha='center')

    plt.show()

    # Save the plot as an image in local storage
    desired_path = os.path.join(ROOT_DIR, "data", "positions","intermediate_dfs","images")
    if not os.path.exists(desired_path):
        os.makedirs(desired_path)
    plt.savefig(os.path.join(desired_path, "positions_distribution_overall.png"))


    # Now let's filter for judges with other positions
    judges_with_other_positions = df[df['person_id'].isin(judges_venn[(judges_venn > 0) & (judges_venn < df.groupby('person_id').size())].index)]
    average_positions_judges_with_others = judges_with_other_positions.groupby('person_id').size().mean()

    # Let's now draw the histogram of the number of positions for judges with other positions
    plt.figure(figsize=(8,6))
    judges_with_other_positions.groupby('person_id').size().plot(kind='hist', bins=100)
    plt.xlabel('Number of positions')
    plt.ylabel('Number of person_ids')
    plt.title('Distribution of positions per judge with other positions')

    # Plot the average value as a vertical line
    plt.axvline(average_positions_judges_with_others, color='r', linestyle='dashed', linewidth=2)
    plt.text(average_positions_judges_with_others, plt.gca().get_ylim()[1] * 0.95, f'Avg: {average_positions_judges_with_others:.2f}', color='r', ha='center')

    plt.show()

    # Save the plot as an image in local storage
    plt.savefig(os.path.join(desired_path, "judges_with_other_positions_distribution.png"))


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
'''