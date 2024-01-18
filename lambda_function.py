import os
import json
import boto3
import uuid
import traceback

from datetime import datetime, timedelta
from yfinance import Ticker

kinesis_client = boto3.client('kinesis')

def lambda_handler(event, context):
    try:
        # data = json.loads(event['body'])

        symbols = os.environ.get('STOCK_SYMBOLS').split(",")
        kinesis_news_records = []

        for symbol in symbols:
            ticker = Ticker(symbol)
            news = ticker.get_news()
            mapped_news = [kinesis_mapper(symbol, news_item) for news_item in news]
            kinesis_news_records.extend(mapped_news)
            
        batch_size = 10
        batches = [kinesis_news_records[i:i+batch_size] for i in range(0, len(kinesis_news_records), batch_size)]
        
        for batch in batches:
          kinesis_client.put_records(
              Records=batch,
              StreamName=os.environ.get('KINESIS_STREAM_NAME'),
              StreamARN=os.environ.get('KINESIS_STREAM_ARN')
          )

        return True
    except Exception as e:
        traceback.print_exc()
        return str(e)
    
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