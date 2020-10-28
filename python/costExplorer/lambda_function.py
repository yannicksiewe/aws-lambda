__author__ = "Yannick Siewe"
__version__ = "0.1.0"
__license__ = "MIT"

import boto3
import datetime
import pandas as pd
from aws_requests_auth.aws_auth import AWSRequestsAuth
from elasticsearch import Elasticsearch, RequestsHttpConnection

ssm = boto3.client('ssm')
session = boto3.session.Session()
credentials = session.get_credentials().get_frozen_credentials()

# Get account ID
account_id = session.client('sts').get_caller_identity().get('Account')

# Specify the svc i'm going to use
client = session.client('ce')

# Define Date interval
now = datetime.datetime.utcnow()
end = now
start = end - datetime.timedelta(days=1)
start = datetime.datetime(year=start.year, month=start.month, day=1)
start = start.strftime('%Y-%m-%d')
end = end.strftime('%Y-%m-%d')


es_host = ssm.get_parameter(Name='es_host', WithDecryption=True)
awsauth = AWSRequestsAuth(
    aws_access_key=credentials.access_key,
    aws_secret_access_key=credentials.secret_key,
    aws_token=credentials.token,
    aws_host=es_host,
    aws_region=session.region_name,
    aws_service='es'
)

# use the requests connection_class and pass in our custom auth class
es = Elasticsearch(
    hosts=[{'host': es_host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


def extractJson():
    requestFile = client.get_cost_and_usage(
        TimePeriod={
            'Start': start,
            'End': end
        },
        Granularity='MONTHLY',
        Metrics=['BlendedCost'],
        GroupBy=[
            {'Type': 'DIMENSION', 'Key': 'SERVICE'},
            {'Type': 'DIMENSION', 'Key': 'REGION'}
        ],
        Filter={
            'And': [{
                'Dimensions': {
                    'Key': 'LINKED_ACCOUNT',
                    'Values': [account_id],
                }
            }, {
                'Not': {
                    'Dimensions': {
                        'Key': 'RECORD_TYPE',
                        'Values': ['Credit', 'Refund']
                    }
                }
            }]
        }
    )
    return requestFile


jsonFile = extractJson()
resources = []
costs = []

for project in jsonFile['ResultsByTime'][0]['Groups']:
    resources.append(project['Keys'][1])
    costs.append(project['Metrics']['BlendedCost']['Amount'])

dataset = {
    'AWS Resource': resources,
    'Blended Cost': costs
}

df = pd.DataFrame.from_dict(dataset)
df['Blended Cost'] = df['Blended Cost'].astype(float)


def return_total_cost(df):
    df_total = df['Blended Cost'].sum().round(3)
    result = '{} usd'.format(df_total)
    return result


total_month_cost = return_total_cost(df)


def buildRequest():

    return {
        'AccountID': account_id,
        'TimePeriod': jsonFile['ResultsByTime'][0]['TimePeriod'],
        'title': 'MONTHLY Cost Repport',
        'Category': jsonFile['ResultsByTime'][0]['Groups'],
        'TotalCost': total_month_cost,
        'timestamp': now
    }


def lambda_handler(event, context):

    try:
        payload = buildRequest()
        print(payload)

        res = es.index(index='demo-index', id=1, body=payload)
        print(res['result'])

        res = es.get(index='demo-index', id=1)
        print(res['_source'])

        es.indices.refresh(index='demo-index')

        res = es.search(index='demo-index', body={'query': {'match_all': {}}})
        print('Got %d Hits:' % res['hits']['total']['value'])
        for hit in res['hits']['hits']:
            print("%(timestamp)s %(title)s: %(TotalCost)s" % hit["_source"])
    except elasticsearch.ElasticsearchException as e:
        print('error')
