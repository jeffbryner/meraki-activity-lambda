# meraki-activity-lambda
an aws lambda to grab meraki activity logs and ship them to firehose


Configuration:
--------------
- FIREHOSE_DELIVERY_STREAM = name of the target firehose stream
- FIREHOSE_BATCH_SIZE =integer for your preferred batch size (100 default)
- MERAKI_API_KEY_NAME = your meraki api key name (get it from your console and save it in secret manager)
- MERAKI_PRODUCT_TYPES = comma separated list of 'product types' you'd like events from:  appliance, camera, switch, wireless