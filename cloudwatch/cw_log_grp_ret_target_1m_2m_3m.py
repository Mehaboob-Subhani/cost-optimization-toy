import boto3
import logging
from datetime import datetime

# List of AWS regions
regions = ['us-east-1', 'us-west-1', 'us-west-2', 'us-east-2']  # Example regions

# Retention period in days (e.g., 30 days)
RETENTION_DAYS = 30
# The target retention we are looking for (2 weeks = 14 days)
TARGET_RETENTION_DAYS = 14

# List of AWS CLI profiles to use (representing different AWS accounts)
aws_profiles = ['accountid']  # Replace with your AWS CLI profile names

# Function to get the AWS Account ID
def get_account_id(profile):
    try:
        sts_client = boto3.Session(profile_name=profile).client('sts')
        response = sts_client.get_caller_identity()
        return response['Account']
    except Exception as e:
        raise Exception(f"Error fetching AWS account ID for profile {profile}: {e}")

# Function to set the retention policy for log groups with retention set to 14 days
def set_retention_for_log_groups(region, profile, logger):
    try:
        # Initialize the session for the region using the specified profile
        session = boto3.Session(profile_name=profile)
        logs_client = session.client('logs', region_name=region)

        # List all log groups
        paginator = logs_client.get_paginator('describe_log_groups')
        for page in paginator.paginate():
            for log_group in page['logGroups']:
                log_group_name = log_group['logGroupName']
                # Check if the retention policy is set to 14 days
                if 'retentionInDays' in log_group and log_group['retentionInDays'] == TARGET_RETENTION_DAYS:
                    logger.info(
                        f"Profile {profile} - Setting retention for log group '{log_group_name}' from {TARGET_RETENTION_DAYS} days to {RETENTION_DAYS} days."
                    )
                    # Update retention policy to 30 days
                    logs_client.put_retention_policy(
                        logGroupName=log_group_name,
                        retentionInDays=RETENTION_DAYS
                    )
                else:
                    logger.info(
                        f"Profile {profile} - Log group '{log_group_name}' has a retention policy of {log_group.get('retentionInDays', 'None')} days. Skipping."
                    )

    except Exception as e:
        logger.error(f"Error processing log groups in region {region} with profile {profile}: {e}")
        logger.exception("Detailed traceback of the exception:")
        raise

# Set up logging with a timestamped log file name
def setup_logging(profile_name):
    # Get the current timestamp and format it as YYYY-MM-DD_HH-MM-SS
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    # Create log file name based on the profile name and timestamp
    log_file_name = f"{profile_name}_{timestamp}_outputlogfile.log"

    # Set up logging with both file and terminal output
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create file handler to log to a file
    file_handler = logging.FileHandler(log_file_name)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # Create console (stream) handler to log to the terminal
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # Add both handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Main processing function to iterate over profiles and regions
def process_log_groups():
    for profile in aws_profiles:
        try:
            # Get the account ID and set up a separate logger for this profile
            logger = setup_logging(profile)
            logger.info(f"Started processing for profile: {profile}")

            # Apply retention policy for all regions for the given profile
            for region in regions:
                logger.info(f"Processing log groups in region {region}...")
                set_retention_for_log_groups(region, profile, logger)

            logger.info(f"Retention policy update complete for profile: {profile}")

        except Exception as e:
            logger.error(f"Error processing log groups for profile {profile}: {e}")
            logger.exception("Detailed traceback of the exception:")

# Run the processing function
process_log_groups()

print("Retention policy update complete. Check the log file for details.")
