from dagster import EnvVar


# #EnvVar defers resolution of the environment variable value until run time, 
# and should only be used as input to Dagster config or resources.
api_token = EnvVar("API_TOKEN")