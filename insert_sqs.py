import boto3

sqs = boto3.client('sqs')

queue_name = 'MyFifoQueue.fifo'
response = sqs.create_queue(
    QueueName=queue_name,
    Attributes={
        'FifoQueue': 'true',
        'ContentBasedDeduplication': 'true',
        'MessageRetentionPeriod': '86400'
    }
)

queue_url = response['QueueUrl']
print(f"Queue URL: {response['QueueUrl']}")

response = sqs.list_queues()
print(f"all queues:{response['QueueUrls']}")

response = sqs.send_message(
    QueueUrl=queue_url,
    MessageAttributes={
        'FileName': {
            'DataType': 'String',
            'StringValue': 'filename1.wav'
        },
        'FileUrl': {
            'DataType': 'String',
            'StringValue': 's3-URL'
        },
    },
    MessageBody=("this is the s3 URL for the wav file"),
    MessageGroupId="someID"
)

print(f"Message sent. MessageId: {response['MessageId']}")

response = sqs.receive_message(
    QueueUrl=queue_url,
    AttributeNames=['SentTimestamp'],
    MaxNumberOfMessages=10,
    MessageAttributeNames=['All'],
    VisibilityTimeout=30,
    WaitTimeSeconds=0
)

messages = response.get('Messages', [])
print(len(messages))
if messages:
    message = messages[0]
    receipt_handle = message['ReceiptHandle']

    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    print(f'Received and deleted message: {message}')
else:
    print('No messages available in the queue.')
