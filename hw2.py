import boto3
import csv

s3 = boto3.resource('s3')

try:
    s3.create_bucket(Bucket='datacont-ktd15', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
except:
    print("Bucket already exists")

s3.Object('datacont-ktd15', 'test.jpg').put(
    Body=open('test.jpg', 'rb')
)

dyndb = boto3.resource('dynamodb', region_name='us-west-2')

try:
    table = dyndb.create_table (
        TableName='DataTable',
        KeySchema=[
            {'AttributeName': 'PartitionKey', 'KeyType': 'HASH'},
            {'AttributeName': 'RowKey', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'PartitionKey', 'AttributeType': 'S'},
            {'AttributeName': 'RowKey', 'AttributeType': 'S'}
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

with open('experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        body = open('datafiles/'+item[3], 'rb')
        s3.Object('datacont-ktd15', item[3]).put(Body=body)
        md = s3.Object('datacont-ktd15', item[3]).Acl().put(ACL='public-read')

        url = "https://s3-us-west-2.amazonaws.com/datacont-ktd15/"+item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
            'description': item[4], 'date': item[2], 'url':url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

response = table.get_item(
    Key={
        'PartitionKey': 'experiment3',
        'RowKey': '4'
        }
    )
item = response['Item']
print(item)