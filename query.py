# retrieve_sensor_data function reads last N entries from a dynamodb table and stores them in a list

import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.table import TableResource
import pandas as pd
import datetime as dt
import argparse


parser = argparse.ArgumentParser(description='Retrieve sensor data from a DynamoDB table')
parser.add_argument('--table_name', type=str, help='Name of the DynamoDB table', default='PFM-DB-SENSOR-DATA')
parser.add_argument('--N', type=int, help='Number of entries to retrieve', default=100)
parser.add_argument('--start_time', type=int, help='Start time in milliseconds since epoch')
parser.add_argument('--end_time', type=int, help='End time in milliseconds since epoch')
parser.add_argument('--output', type=str, help='Output file name (default: output.csv)')
parser.add_argument('--profile', type=str, help='AWS profile to use (default: default)', default='default')
parser.add_argument('--region', type=str, help='AWS region to use (default: us-east-2)', default='us-east-2')
parser.add_argument('--all', action='store_true', help='Retrieve all entries from the table')

args = parser.parse_args()

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
    

def get_table_items_count(table_name):
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.scan(Select='COUNT')
    return response['Count']

def retrieve_sensor_data(table_name, N, start_time: int, end_time: int):
    # retrieve partition_key = time between start_time and end_time
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(table_name)
    items = []
    table
    if args.all:
        # dump all the entries from the table
        response = table.scan()
        items.extend(response['Items'])

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                Select='ALL_ATTRIBUTES',
                ExclusiveStartKey=response['LastEvaluatedKey'])

            items.extend(response['Items'])
            print(len(items))

            if len(items) > N:
                break
    else:
        # scan retrieving the last N entries
        response = table.scan(
            Limit=N,
            FilterExpression=Key('time').between(start_time, end_time),
        )
        items = response['Items']
      

   
    return items

def to_df(items) -> pd.DataFrame:
    values = []
    for item in items:
        data = item['device_data']['state']['reported']
        time = data['gen']['time']

        # time is a string in the format 'YYYYMMDDHHMMSS'
        # we need to convert it to a string in the format 'YYYY-MM-DD HH:MM:SS'
        time = time[:4] + '-' + time[4:6] + '-' + time[6:8] + ' ' + time[8:10] + ':' + time[10:12] + ':' + time[12:14]

        time = dt.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')

        values.append({
            'timestamp': time,
            'MOTOR SPEED-X [mm/sec]': data['regs']['MOTOR INPUTS 5'],
            'MOTOR SPEED-Z [mm/sec]': data['regs']['MOTOR INPUTS 1'],
            'MOTOR TEMP [F°]': data['regs']['MOTOR INPUTS 3'],
            'TURBINE SPEED-X [mm/sec]': data['regs']['TURBINA INPUTS 5'],
            'TURBINE SPEED-Z [mm/sec]': data['regs']['TURBINA INPUTS 1'],
            'TURBINE TEMP [F°]': data['regs']['TURBINA INPUTS 3'],
        })

    # make a dataframe from the list of values
    return pd.DataFrame(values)

if __name__ == '__main__':
    table_name = 'PFM-DB-SENSOR-DATA'

    # print(f'Number of items in the table: {get_table_items_count(table_name)}')

    N = 1_000_000
    start = 1709294400000
    items = retrieve_sensor_data(table_name, N, start, start + 4*3600*1000)
    df = to_df(items)

    print(len(df))
    # sort the dataframe by timestamp
    df = df.sort_values(by='timestamp')
    # print(df)
    if args.output is not None:
        df.to_excel(args.output, index=False)
