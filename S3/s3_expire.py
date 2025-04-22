import boto3
import csv
import logging
import sys
from datetime import datetime
from botocore.exceptions import ClientError

# Common Variables
LIFECYCLE_DAYS = 60  # Set how many days after which objects should expire
LOG_FILE_NAME = f"s3_lifecycle_policy_expire_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
AWS_PROFILES = ['accountid']  # List of AWS CLI profiles you want to use

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
    """Create the S3 lifecycle policy to expire objects after X days."""
    lifecycle_policy_name = f'{bucket_name}_lifecycle_policy_expire'

    lifecycle_config = {
        'Rules': [
            {
                'ID': lifecycle_policy_name,
                'Filter': {},
                'Status': 'Enabled',
                'Expiration': {'Days': LIFECYCLE_DAYS},
                'NoncurrentVersionExpiration': {'NoncurrentDays': LIFECYCLE_DAYS},
            }
        ]
    }

    try:
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
                        # Raise an exception if the bucket is incorrect or inaccessible
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