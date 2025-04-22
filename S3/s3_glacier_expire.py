import boto3
import csv
import logging
import sys
from datetime import datetime
from botocore.exceptions import ClientError

# Common Variables
LIFECYCLE_DAYS_TO_GLACIER = 180  # Days after which objects should transition to Glacier
LIFECYCLE_DAYS_TO_EXPIRATION = 365  # Days after which objects should expire
LIFECYCLE_NONCURRENT_DAYS_TO_GLACIER = 180  # Days after which non-current versions should transition to Glacier
LIFECYCLE_NONCURRENT_DAYS_TO_EXPIRATION = 365  # Days after which non-current versions should expire
GLACIER_STORAGE_CLASS = 'GLACIER_IR'  # Set to 'GLACIER_IR' or 'GLACIER' for transition
LOG_FILE_NAME = f"s3_lifecycle_policy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
AWS_PROFILES = ['account_id']  # List of AWS CLI profiles you want to use

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[ 
        logging.StreamHandler(),  # Print logs to console
        logging.FileHandler(LOG_FILE_NAME)  # Save logs to file
    ]
)

def create_lifecycle_policy(bucket_name, s3_client, profile):
    """Create the S3 lifecycle policy to transition objects to Glacier and then expire them."""
    lifecycle_policy_name = f'{bucket_name}_lifecycle_policy'

    try:
        lifecycle_config = {
            'Rules': [
                {
                    'ID': lifecycle_policy_name,
                    'Filter': {},  # Empty filter applies to all objects in the bucket
                    'Status': 'Enabled',
                    'Transitions': [
                        {
                            'Days': LIFECYCLE_DAYS_TO_GLACIER,
                            'StorageClass': GLACIER_STORAGE_CLASS
                        }
                    ],
                    'Expiration': {
                        'Days': LIFECYCLE_DAYS_TO_EXPIRATION
                    },
                    'NoncurrentVersionTransitions': [
                        {
                            'NoncurrentDays': LIFECYCLE_NONCURRENT_DAYS_TO_GLACIER,
                            'StorageClass': GLACIER_STORAGE_CLASS
                        }
                    ],
                    'NoncurrentVersionExpiration': {
                        'NoncurrentDays': LIFECYCLE_NONCURRENT_DAYS_TO_EXPIRATION
                    }
                }
            ]
        }

        # Apply the lifecycle configuration to the bucket
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config
        )
        logging.info(f"Profile {profile} - Lifecycle policy created successfully for bucket: {bucket_name}")

    except ClientError as e:
        logging.error(f"Profile {profile} - Error creating lifecycle policy for bucket {bucket_name}: {e}")
        raise

def process_buckets(csv_file_path):
    """Process the CSV file with bucket names and apply lifecycle policies."""
    try:
        with open(csv_file_path, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                bucket_name = row[0].strip()
                if not bucket_name:
                    continue

                logging.info(f"Processing bucket: {bucket_name}")
                for profile in AWS_PROFILES:
                    session = boto3.Session(profile_name=profile)
                    s3_client = session.client('s3')

                    # Log the profile/account being used
                    logging.info(f"Profile {profile} - Using AWS Profile")

                    # Check if the bucket exists
                    try:
                        s3_client.head_bucket(Bucket=bucket_name)
                        # If the bucket exists, create the lifecycle policy
                        create_lifecycle_policy(bucket_name, s3_client, profile)
                    except ClientError as e:
                        logging.error(f"Profile {profile} - Bucket {bucket_name} does not exist or cannot be accessed: {e}")
                        raise Exception(f"Bucket {bucket_name} is not accessible or doesn't exist.")
    except FileNotFoundError:
        logging.error(f"CSV file not found: {csv_file_path}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error while processing buckets: {e}")
        raise

def main():
    """Main function to run the lifecycle policy creation."""
    if len(sys.argv) < 2:
        logging.error("Usage: python create_s3_lifecycle_policy.py <path_to_csv>")
        sys.exit(1)
    
    csv_file_path = sys.argv[1]
    logging.info("Starting the lifecycle policy creation process...")
    
    try:
        process_buckets(csv_file_path)
        logging.info("Lifecycle policy creation process completed.")
    except Exception as e:
        logging.error(f"Process terminated due to error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
