import boto3
import json
import os
import logging
import requests
from botocore.exceptions import ClientError
from datetime import datetime,timedelta
from utils.helpers import chunks
from utils.dates import utcnow


logger = logging.getLogger()
logger.setLevel(logging.INFO)

FIREHOSE_DELIVERY_STREAM= os.environ.get('FIREHOSE_DELIVERY_STREAM','test')
FIREHOSE_BATCH_SIZE=os.environ.get('FIREHOSE_BATCH_SIZE',100)
MERAKI_API_KEY=os.environ.get('MERAKI_API_KEY','unknown')
MERAKI_PRODUCT_TYPES=os.environ.get('MERAKI_PRODUCT_TYPES','wireless')
ssmclient=boto3.client('ssm')
secrets_manager = boto3.client('secretsmanager')
f_hose = boto3.client('firehose')

def get_parameter(parameter_name,default):
    try:
        return(ssmclient.get_parameter(Name=parameter_name)["Parameter"]['Value'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'ParameterNotFound':
            return default

def put_parameter(parameter_name,value):
    ssmclient.put_parameter(Name=parameter_name,Type='String',Value=value,Overwrite=True)

def send_to_firehose(records):
    # records should be a list of dicts
    if type(records) is list:
        # batch up the list below the limits of firehose
        for batch in chunks(records,FIREHOSE_BATCH_SIZE):
            response = f_hose.put_record_batch(
                DeliveryStreamName=FIREHOSE_DELIVERY_STREAM,
                Records=[{'Data': bytes(str(json.dumps(record)+'\n').encode('UTF-8'))} for record in batch]
            )
            logger.debug('firehose response is: {}'.format(response))

def handler(event,context):
    # get the api key
    meraki_api_key = secrets_manager.get_secret_value(SecretId=MERAKI_API_KEY)["SecretString"]

    # setup a session
    session = requests.Session()
    session.headers = {'Content-Type': 'application/json',
                            'X-Cisco-Meraki-API-Key': meraki_api_key}
    # get the org id
    url='https://api.meraki.com/api/v0/organizations'
    result=session.get(url)
    organizationId=result.json()[0]['id']

    # get the networks associated with this org
    url=f'https://api.meraki.com/api/v0/organizations/{organizationId}/networks'
    result=session.get(url)
    networks=json.loads(result.json())
    last_run_time = utcnow().isoformat()
    records_retrieved = False
    per_page=1000
    # Meraki has 'productTypes': ['appliance', 'camera', 'switch', 'wireless']
    # what event/product types do we want?
    event_set=set(MERAKI_PRODUCT_TYPES.split(","))

    for network in networks:
        network_id=network["id"]
        # get events since last time we ran (default an hour ago)
        start_after=get_parameter('/meraki-events/lastquerytime',(utcnow()-timedelta(minutes=60)).isoformat())
        # reformat date string to what Meraki likes
        # Z instead of +00:00
        start_after='{}'.format(start_after.replace('+00:00','Z'))
        logger.info(f"Looking for records since: {start_after}")

        product_set=set(network["productTypes"])
        # if it's not wireless, move on
        for event_type in event_set.intersection(product_set):
            next_page = True
            while next_page:
                url=f'https://api.meraki.com/api/v0/networks/{network_id}/events?productType={event_type}&perPage={per_page}&startingAfter={start_after}'
                result=session.get(url)
                events=result.json()['events']
                start_after=result.json()['pageEndAt']
                if len(events) == 0:
                    next_page = False
                else:
                    logger.info(f"sending: {len(events)} meraki records to firehose")
                    send_to_firehose(events)
                    records_retrieved = True

    # sometimes activity log lags behind realtime
    # so regardless of the time we request, there won't be records available until later
    # only move the time forward if we received records.
    if records_retrieved:
        put_parameter('/meraki-events/lastquerytime',last_run_time)
