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


def lambda_handler(event, context) :

    # Data Lists
    stock_symbols = [
    "ADM",   # Archer-Daniels-Midland Company
    "BG",    # Bunge Limited
    "CTVA",  # Corteva, Inc.
    "DE",    # Deere & Company
    "TSN",   # Tyson Foods, Inc.
    "MOS",   # Mosaic Company
    "CF",    # CF Industries Holdings, Inc.
    "AGCO",  # AGCO Corporation
    "PPC",   # Pilgrim's Pride Corporation
    "CALM",  # Cal-Maine Foods, Inc.
    "INGR",  # Ingredion Incorporated
    "FDP",   # Fresh Del Monte Produce Inc.
    "ANDE",  # Andersons, Inc.
    "LNN",   # Lindsay Corporation
    "SMG",   # Scotts Miracle-Gro Company
    "CVGW",  # Calavo Growers, Inc.
    "DAR",   # Darling Ingredients Inc.
    "SEB",   # Seaboard Corporation
    "HRL",   # Hormel Foods Corporation
    "TSCO",  # Tractor Supply Company
    "ADM",   # Archer-Daniels-Midland Company
    "BG",    # Bunge Limited
    "CAG",   # Conagra Brands, Inc.
    "MKC",   # McCormick & Company, Incorporated
    "HOGS",  # Zhongpin Inc.
    "SYY",   # Sysco Corporation
    "BRFS",  # BRF S.A.
    "HAIN",  # Hain Celestial Group, Inc.
    "MDLZ",  # Mondelez International, Inc.
    "UL",    # Unilever PLC
    "KHC",   # Kraft Heinz Company
    "DAR",   # Darling Ingredients Inc.
    "JBSAY", # JBS S.A.
    "CALM",  # Cal-Maine Foods, Inc.
    "TSN",   # Tyson Foods, Inc.
    "PFGC",  # Performance Food Group Company
    "SENEA", # Seneca Foods Corporation
    "FLO",   # Flowers Foods, Inc.
    "LANC",  # Lancaster Colony Corporation
    "POST",  # Post Holdings, Inc.
    "JVA",   # Coffee Holding Co., Inc.
    "STKL",  # SunOpta Inc.
    "SMPL",  # The Simply Good Foods Company
    "THS",   # TreeHouse Foods, Inc.
    "UNFI",  # United Natural Foods, Inc.
    "WW",    # WW International, Inc.
    "BGS",   # B&G Foods, Inc.
    "JJSF",  # J&J Snack Foods Corp.
    "ALCO",  # Alico, Inc.
    "ANDE",  # The Andersons, Inc.
    "SEB",   # Seaboard Corporation
    ]

    ENCRYPTED = os.environ['ALPHAVANTAGE_API_KEY']  # Retrieved from AWS Secrets Manager

    # Decrypt code should run once and variables stored outside of the function
    # handler so that these are decrypted once per container
    DECRYPTED = boto3.client('kms').decrypt(
        CiphertextBlob=b64decode(ENCRYPTED),
        EncryptionContext={'LambdaFunctionName' : os.environ['AWS_LAMBDA_FUNCTION_NAME']}
    )['Plaintext'].decode('utf-8')


    api_key = DECRYPTED
    outputsize = 'full'  # Fetch full data
    max_api_calls_per_day = 55  # Set limit for API calls
    api_calls = 0

    for symbol in stock_symbols :
        if api_calls >= max_api_calls_per_day :
            logger.info("Max API call limit reached. Stopping process.")
            break

        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize={outputsize}&datatype=json&apikey={api_key}'

        try :
            logger.info(f"Fetching data for {symbol}")
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for bad requests

            stock_data = response.json()

            # Transforming stock data - extracting necessary fields
            if 'Time Series (Daily)' in stock_data :
                csv_buffer = StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow(['symbol', 'date', 'open', 'high', 'low', 'close', 'volume'])  # CSV header

                for date, daily_data in stock_data['Time Series (Daily)'].items() :
                    csv_writer.writerow([
                        symbol,
                        date,
                        daily_data['1. open'],
                        daily_data['2. high'],
                        daily_data['3. low'],
                        daily_data['4. close'],
                        daily_data['5. volume']
                    ])

                # Define the S3 file path and bucket
                s3_bucket = 'hurricane-agriculture-data'
                s3_key = f'stocks/{symbol}_historical_daily_data.csv'

                # Upload CSV file to S3
                logger.info(f"Uploading {symbol} data to S3 as {s3_key}")
                s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())

                logger.info(f"Uploaded {symbol} data to S3 successfully.")

            else :
                logger.error(f"No 'Time Series (Daily)' data found for {symbol}.")

            api_calls += 1
            time.sleep(5)  # Avoid hitting the rate limit

        except requests.exceptions.RequestException as e :
            logger.error(f"Error fetching data for {symbol}: {e}")
        except ClientError as e :
            logger.error(f"Error uploading {symbol} data to S3: {e}")

    return {
        'statusCode' : 200,
        'body' : 'Stock data pull and upload to S3 completed.'
    }


