import os
import json
import boto3
import uuid
import logging
import traceback

from datetime import datetime, timedelta
from yfinance import Ticker

logger = logging.getLogger()
logger.setLevel(logging.INFO)

kinesis_client = boto3.client('kinesis')

def get_stock_news():
    try:
        logger.info("INFO: Attempting to retrieve stock news data from yfinance..")

        symbols = os.environ.get('STOCK_SYMBOLS').split(",")
        kinesis_news_records = []

        for symbol in symbols:
            ticker = Ticker(symbol)
            news = ticker.get_news()

            twenty_four_hours_ago = datetime.now() - timedelta(hours=12)
            timestamp_threshold = int(twenty_four_hours_ago.timestamp())
            news_last_day = [story for story in news if story['providerPublishTime'] >= timestamp_threshold]

            mapped_news = [kinesis_mapper(symbol, news_item) for news_item in news_last_day]
            kinesis_news_records.extend(mapped_news)

        logger.info("SUCCESS: Stock news retrieved from yfinance.")

        return kinesis_news_records
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not retrieve stock news data from yfinance.")
        logger.error(e)

def lambda_handler(event, context):
    try:
        logger.info("INFO: Attempting to add records to Kinesis..")

        kinesis_news_records = get_stock_news()

        batch_size = 10
        batches = [kinesis_news_records[i:i+batch_size] for i in range(0, len(kinesis_news_records), batch_size)]
        
        for batch in batches:
          kinesis_client.put_records(
              Records=batch,
              StreamName=os.environ.get('KINESIS_STREAM_NAME'),
              StreamARN=os.environ.get('KINESIS_STREAM_ARN')
          )

        logger.info("SUCCESS: Stock news records added to Kinesis.")

        return True
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not add records to Kinesis.")
        logger.error(e)
    
def kinesis_mapper(symbol, news):
    collection_time_str = datetime.now().isoformat()

    data = {
        'symbol': symbol,
        'collection_time': collection_time_str,
        'title': news['title'],
        'link': news['link'],
        'publisher': news['publisher']
    }

    return {
        'Data': json.dumps(data),
        'PartitionKey': str(uuid.uuid4())
    }