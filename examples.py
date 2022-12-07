import pandas as pd
from pandas  import json_normalize
from tabulate import tabulate
from flatten_json import flatten, flatten_json

from flattern import NotADictionary

now = pd.datetime.datetime.now()

print(now
      )
def flatten(obj: dict) -> dict:
    """
    This function takes a dictionary with arbitrary levels of nested
    lists and dictionaries and flattens it.
    Raises NotADictionary if the input is invalid.
    """

    if not isinstance(obj, dict):
        raise NotADictionary

    flattened_data = {}

    def flatten_json(json_data, name=''):
        if type(json_data) is dict:
            for key in json_data:
                if not bool(json_data[key]):  # handle empty dict values
                    flattened_data[name + key + '.'[:-1]] = json_data[key]
                else:
                    flatten_json(json_data[key], name + key + '.')
        elif type(json_data) is list:
            i = 0
            for key in json_data:
                flatten_json(key, name + str(i) + '.')
                i += 1
        else:
            flattened_data[name[:-1]] = json_data

    try:
        flatten_json(obj)

    except Exception as e:
        raise e

    return flattened_data


json_obj = {
    "executionPlan": {
        "_id": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07",
        "name": "Databricks Shell",
        "systemInfo": {
            "name": "spark",
            "version": "3.3.0"
        },
        "agentInfo": {
            "name": "spline",
            "version": "0.7.12"
        },
        "extra": {
            "appName": "Databricks Shell",
            "attributes": [
                {
                    "id": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07:attr-0",
                    "name": "circuitId",
                    "dataTypeId": "e63adadc-648a-56a0-9424-3289858cf0bb"
                },
                {
                    "id": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07:attr-1",
                    "name": "circuitRef",
                    "dataTypeId": "75fe27b9-9a00-5c7d-966f-33ba32333133"
                },
                {
                    "id": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07:attr-2",
                    "name": "name",
                    "dataTypeId": "75fe27b9-9a00-5c7d-966f-33ba32333133"
                },
                {
                    "id": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07:attr-3",
                    "name": "location",
                    "dataTypeId": "75fe27b9-9a00-5c7d-966f-33ba32333133"
                },
                {
                    "id": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07:attr-4",
                    "name": "country",
                    "dataTypeId": "75fe27b9-9a00-5c7d-966f-33ba32333133"
                },
                {
                    "id": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07:attr-5",
                    "name": "lat",
                    "dataTypeId": "a155e715-56ab-59c4-a94b-ed1851a6984a"
                },
                {
                    "id": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07:attr-6",
                    "name": "lng",
                    "dataTypeId": "a155e715-56ab-59c4-a94b-ed1851a6984a"
                },
                {
                    "id": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07:attr-7",
                    "name": "alt",
                    "dataTypeId": "e63adadc-648a-56a0-9424-3289858cf0bb"
                },
                {
                    "id": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07:attr-8",
                    "name": "url",
                    "dataTypeId": "75fe27b9-9a00-5c7d-966f-33ba32333133"
                }
            ],
            "dataTypes": [
                {
                    "_typeHint": "dt.Simple",
                    "id": "e63adadc-648a-56a0-9424-3289858cf0bb",
                    "name": "int",
                    "nullable": "true"
                },
                {
                    "_typeHint": "dt.Simple",
                    "id": "75fe27b9-9a00-5c7d-966f-33ba32333133",
                    "name": "string",
                    "nullable": "true"
                },
                {
                    "_typeHint": "dt.Simple",
                    "id": "a155e715-56ab-59c4-a94b-ed1851a6984a",
                    "name": "double",
                    "nullable": "true"
                }
            ]
        },
        "inputs": [
            {
                "sourceType": "csv",
                "source": "dbfs:/FileStore/tables/circuits-2.csv"
            }
        ],
        "output": {
            "sourceType": "csv",
            "source": "dbfs:/FileStore/tables/ind-2.csv"
        }
    },
    "graph": {
        "nodes": [
            {
                "_id": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07:op-1",
                "_type": "Read",
                "name": "LogicalRelation",
                "properties": "null"
            },
            {
                "_id": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07:op-0",
                "_type": "Write",
                "name": "InsertIntoHadoopFsRelationCommand",
                "properties": "null"
            }
        ],
        "edges": [
            {
                "source": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07:op-1",
                "target": "fdd38b4a-493f-5b9e-b462-28c07ac7fc07:op-0"
            }
        ]
    }
}


jsonSample = [
    {
        'class': 'Year 1',
        'student count': 20,
        'room': 'Yellow',
        'info': {
            'teachers': {
                'math': 'Rick Scott',
                'physics': 'Elon Mask'
            }
        },
        'students': [
            {
                'name': 'Tom',
                'sex': 'M',
                'grades': { 'math': 66, 'physics': 77 }
            },
            {
                'name': 'James',
                'sex': 'M',
                'grades': { 'math': 80, 'physics': 78 }
            },
        ]
    },
    {
        'class': 'Year 2',
        'student count': 25,
        'room': 'Blue',
        'info': {
            'teachers': {
                'math': 'Alan Turing',
                'physics': 'Albert Einstein'
            }
        },
        'students': [
            { 'name': 'Tony', 'sex': 'M' },
            { 'name': 'Jacqueline', 'sex': 'F' },
        ]
    },
]

df=pd.json_normalize(json_obj)



#
#
#
# #print(tabulate(df, headers='keys', tablefmt='psql'))
#
#
# result=pd.json_normalize(json_obj, record_path=['executionPlan','extra','attributes'])
#
#
#
# #df=pd.json_normalize(json_obj, record_path=['executionPlan.extra.attributes'],meta=['executionPlan', ['_id', 'name']],)
#
# print(tabulate(result, headers='keys', tablefmt='psql'))
#
# result=pd.json_normalize(json_obj, record_path=['executionPlan','extra','dataTypes'])
# print(tabulate(result, headers='keys', tablefmt='psql'))
#
# dx=flatten(json_obj)
#
# result=pd.json_normalize(dx)
# print(tabulate(result, headers='keys', tablefmt='psql'))

import pandas as pd

d1 = {
  "id": [420, 380, 390],
  "duration": ['50', '40', '45']
}


d2 = {
  "id": [420, 380],
  "duration": ['50', '70']
}


#load data into a DataFrame object:
df1 = pd.DataFrame(d1)
df2 = pd.DataFrame(d2)


print(tabulate(df1, headers='keys', tablefmt='psql'))

print(tabulate(df2, headers='keys', tablefmt='psql'))


#print(df)


result=(pd.merge(df1, df2, left_on='id', right_on='id', how='left'))

#new_df = pd.merge(A_df, B_df,  how='left', left_on=['A_c1','c2'], right_on = ['B_c1','c2'])


rs=result[['duration_y']]
print(tabulate(result, headers='keys', tablefmt='psql'))

print(tabulate(rs, headers='keys', tablefmt='psql'))



# df=pd.DataFrame(dx,index=[0])
#
# print(tabulate(df, headers='keys', tablefmt='psql'))
# dfx=pd.json_normalize(dic)
#
# print(tabulate(dfx, headers='keys', tablefmt='psql'))
#
#dic_flattened = [flatten(d) for d in dic]
#
#
#
#df = pd.DataFrame(dx)

#df.head()