import boto3
import requests
import csv
import os
import time
import logging
from io import StringIO
from botocore.exceptions import ClientError
from base64 import b64decode

# Initialize S3 client
s3 = boto3.client('s3')

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    # Retrieve the encrypted API key from AWS Secrets Manager
    ENCRYPTED = os.environ['ALPHAVANTAGE_API_KEY']
    DECRYPTED = boto3.client('kms').decrypt(
        CiphertextBlob=b64decode(ENCRYPTED),
        EncryptionContext={'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
    )['Plaintext'].decode('utf-8')

    api_key = DECRYPTED
    max_api_calls_per_day = 25  # Set limit for API calls
    api_calls = 0
    s3_bucket = 'hurricane-agriculture-data'

    # List of commodities
    commodities = ["WHEAT", "CORN", "COTTON", "SUGAR", "COFFEE"]

    # List of economic indicators
    indicators = [
        "CPI",
        "RETAIL_SALES",
        "UNEMPLOYMENT",
        "NONFARM_PAYROLL"
    ]

    # Fetch and upload commodity data
    for commodity in commodities:
        if api_calls >= max_api_calls_per_day:
            logger.info("Max API call limit reached. Stopping process.")
            break

        url = f'https://www.alphavantage.co/query?function={commodity}&interval=monthly&apikey={api_key}'

        try:
            logger.info(f"Fetching data for {commodity}")
            responses_com = requests.get(url)
            responses_com.raise_for_status()

            commodities_data = responses_com.json()

            # Check if 'data' is a list or a dictionary
            if 'data' in commodities_data:
                if isinstance(commodities_data['data'], list):
                    # If 'data' is a list, process it differently
                    csv_buffer = StringIO()
                    csv_writer = csv.writer(csv_buffer)
                    csv_writer.writerow(['commodity', 'date', 'value'])  # CSV header

                    for item in commodities_data['data']:
                        # Assuming each item is a dictionary with 'date' and 'value' keys
                        date = item.get('date', 'N/A')
                        value = item.get('value', 'N/A')
                        csv_writer.writerow([commodity, date, value])

                elif isinstance(commodities_data['data'], dict):
                    csv_buffer = StringIO()
                    csv_writer = csv.writer(csv_buffer)
                    csv_writer.writerow(['commodity', 'date', 'value'])  # CSV header

                    for date, value in commodities_data['data'].items():
                        csv_writer.writerow([commodity, date, value])

                # Upload CSV file to S3 after processing all data for the commodity
                s3_key_comm = f'commodities/{commodity}_historical_monthly_data.csv'
                logger.info(f"Uploading {commodity} data to S3 as {s3_key_comm}")
                s3.put_object(Bucket=s3_bucket, Key=s3_key_comm, Body=csv_buffer.getvalue())

                logger.info(f"Uploaded {commodity} data to S3 successfully.")

            else:
                logger.error(f"No 'data' found for {commodity}.")

            api_calls += 1
            time.sleep(5)  # Avoid hitting rate limits

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {commodity}: {e}")
        except ClientError as e:
            logger.error(f"Error uploading {commodity} data to S3: {e}")

    # Fetch and upload economic indicator data
    for indicator in indicators:
        if api_calls >= max_api_calls_per_day:
            logger.info("Max API call limit reached. Stopping process.")
            break

        if indicator == "CPI":
            url = f'https://www.alphavantage.co/query?function={indicator}&interval=monthly&apikey={api_key}'
        else:
            url = f'https://www.alphavantage.co/query?function={indicator}&apikey={api_key}'

        try:
            logger.info(f"Fetching data for {indicator}")
            response_ind = requests.get(url)
            response_ind.raise_for_status()

            indicator_data = response_ind.json()

            # Check if 'data' is a list or a dictionary
            if 'data' in indicator_data:
                if isinstance(indicator_data['data'], list):
                    csv_buffer = StringIO()
                    csv_writer = csv.writer(csv_buffer)
                    csv_writer.writerow(['indicator', 'date', 'value'])  # CSV header

                    for item in indicator_data['data']:
                        date = item.get('date', 'N/A')
                        value = item.get('value', 'N/A')
                        csv_writer.writerow([indicator, date, value])

                elif isinstance(indicator_data['data'], dict):
                    csv_buffer = StringIO()
                    csv_writer = csv.writer(csv_buffer)
                    csv_writer.writerow(['indicator', 'date', 'value'])  # CSV header

                    for date, value in indicator_data['data'].items():
                        csv_writer.writerow([indicator, date, value])

                # Upload CSV file to S3 after processing all data for the indicator
                s3_key_ind = f'economic_indicators/{indicator}_historical_monthly_data.csv'
                logger.info(f"Uploading {indicator} data to S3 as {s3_key_ind}")
                s3.put_object(Bucket=s3_bucket, Key=s3_key_ind, Body=csv_buffer.getvalue())

                logger.info(f"Uploaded {indicator} data to S3 successfully.")

            else:
                logger.error(f"No 'data' found for {indicator}.")

            api_calls += 1
            time.sleep(5)  # Avoid hitting rate limits

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {indicator}: {e}")
        except ClientError as e:
            logger.error(f"Error uploading {indicator} data to S3: {e}")

    # Fetch and upload inflation data
    if api_calls < max_api_calls_per_day :
        url = f'https://www.alphavantage.co/query?function=INFLATION&apikey={api_key}'

        try :
            logger.info("Fetching inflation data")
            response_inflation = requests.get(url)
            response_inflation.raise_for_status()

            inflation_data = response_inflation.json()

            # Check if 'data' is a list
            if 'data' in inflation_data and isinstance(inflation_data['data'], list) :
                csv_buffer = StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow(['date', 'inflation_rate'])  # CSV header

                for item in inflation_data['data'] :
                    date = item.get('date', 'N/A')
                    value = item.get('value', 'N/A')
                    csv_writer.writerow([date, value])

                # Upload CSV file to S3 after processing inflation data
                s3_key_inflation = 'inflation/inflation_historical_yearly_data.csv'
                logger.info(f"Uploading inflation data to S3 as {s3_key_inflation}")
                s3.put_object(Bucket=s3_bucket, Key=s3_key_inflation, Body=csv_buffer.getvalue())

                logger.info("Uploaded inflation data to S3 successfully.")

            else :
                logger.error("No 'data' found for inflation.")

            api_calls += 1
            time.sleep(5)  # Avoid hitting rate limits

        except requests.exceptions.RequestException as e :
            logger.error(f"Error fetching inflation data: {e}")
        except ClientError as e :
            logger.error(f"Error uploading inflation data to S3: {e}")

    return {
        'statusCode': 200,
        'body': 'Commodity and Economic Indicator data pull and upload to S3 completed.'
    }
