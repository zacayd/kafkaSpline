import hashlib
import os

import pandas as pd
import requests
import json
class OrientDBApi:
    def __init__(self, orientURL,databaseName,dictFileName,dictFileNameEdges):
        self.orientURL = orientURL
        self.databaseName=databaseName
        self.dictFileName=dictFileName
        self.dictFileNameEdges=dictFileNameEdges
        self.dictRids={}
        self.dictEdges={}


        if os.path.exists(dictFileName):
            with open(dictFileName) as user_file:
                file_contents = user_file.read()
                parsed_json = json.loads(file_contents)
                self.dictRids =parsed_json

        if os.path.exists(dictFileNameEdges):
            with open(dictFileNameEdges) as user_file:
                file_contents = user_file.read()
                parsed_json = json.loads(file_contents)
                self.dictEdges = parsed_json



        if (not self.CheckIfDbExists()):
            url = f'{orientURL}/database/{databaseName}/plocal'


            payload = {}
            headers = {
                'Authorization': 'Basic cm9vdDpyb290'

            }

            response = requests.request("POST", url, headers=headers, data=payload)
            if (response.status_code==200):
                if str(databaseName).split('_')[-1]=='E2E':
                    self.RunApiPost("CREATE CLASS COLUMN EXTENDS V","POST")
                    self.RunApiPost("CREATE CLASS DATAFLOW EXTENDS E","POST")
                else:
                    self.RunApiPost("CREATE CLASS LINEAGEOBJECT EXTENDS V","POST")
                    self.RunApiPost("CREATE CLASS LINEAGEFLOW EXTENDS E","POST")
            else:
                print(response.text)



            print(response.text)
        else:
            print(f'Database {databaseName} already exists!')



    def CheckIfDbExists(self):


        url = f'{self.orientURL}/database/{self.databaseName}'

        payload = {}
        headers = {
            'Authorization': 'Basic cm9vdDpyb290'

        }

        response = requests.request("GET", url, headers=headers, data=payload)

        print(response.text)
        exists=False
        dictResult=json.loads(response.text)
        try:

            if 'error' in dictResult:
                exists=False
            elif 'server' in dictResult:
                return  True
        except  Exception as e:
           print(e)
           return  False


    def RunApiPost(self,command,method):
        url = f'{self.orientURL}/command/{self.databaseName}/sql'

        payload = json.dumps({
            "command": command
        })
        headers = {
            'Authorization': 'Basic cm9vdDpyb290',
            'Content-Type': 'application/json'

        }

        response = requests.request(method, url, headers=headers, data=payload)

        print(response.text)
        x="-1"
        if(response.status_code==200):
            if "@rid" in json.loads(response.text)["result"][0]:
                x= json.loads(response.text)["result"][0]["@rid"]
            else:
                x = json.loads(response.text)["result"][0]

        return x




    def CreateVertices(self,df):
        for index, row in df.iterrows():

            try:

                ObjectID_1 = row["SourceDB"] + row["SourceSchema"] + row["SourceTableName"] + row[
                    'SourceColumn']
                ObjectGUID_1 = hashlib.md5(ObjectID_1.encode('utf-8')).hexdigest()

                ObjectID_2 = row["ContainerObjectPath"] + row["ControlFlowPath"] + row[
                    "sourceLayerName"] + row['SourceColumn']
                ObjectGUID_2 = hashlib.md5(ObjectID_2.encode('utf-8')).hexdigest()

                ###
                ObjectID_3 = row["TargetDB"] + row["TargetSchema"] + row["TargetTableName"] + row[
                    'TargetColumn']
                ObjectGUID_3 = hashlib.md5(ObjectID_3.encode('utf-8')).hexdigest()

                ObjectID_4 = row["ContainerObjectPath"] + row["ControlFlowPath"] + row[
                    "TargetLayerName"] + row['TargetColumn']
                ObjectGUID_4 = hashlib.md5(ObjectID_4.encode('utf-8')).hexdigest()

                listOfParams1 = [row["ConnectionID"], row["ConnLogicName"], row["ContainerToolType"],
                                row["ContainerObjectName"],
                                row["ContainerObjectPath"], row["ContainerObjectType"], row["ControlFlowName"],
                                row["ControlFlowPath"],
                                ObjectID_1, ObjectGUID_1, row["SourceObjectType"], row["SourceColumn"],
                                row["SourceDataType"], row["sourceLayerName"],
                                row['SourceSchema'], row['SourceDB'], row['SourceTableName'], row["SourceIsObjectData"],
                                1, 0, 1, str(pd.datetime.datetime.now()),'DATABASE'
                                ]
                listOfParams2 =[row["ConnectionID"], row["ConnLogicName"], row["ContainerToolType"],
                                row["ContainerObjectName"],
                                row["ContainerObjectPath"], row["ContainerObjectType"], row["ControlFlowName"],
                                row["ControlFlowPath"],
                                ObjectID_2, ObjectGUID_2, row["SourceObjectType"], row["SourceColumn"],
                                row["SourceDataType"], row["sourceLayerName"],
                                row['SourceSchema'], row['SourceDB'], row['SourceTableName'], row["SourceIsObjectData"],
                                0, 1, 1, str(pd.datetime.datetime.now()),'ETL'
                                ]

                listOfParams3 = [row["ConnectionID"], row["ConnLogicName"], row["ContainerToolType"],
                                row["ContainerObjectName"],
                                row["ContainerObjectPath"], row["ContainerObjectType"], row["ControlFlowName"],
                                row["ControlFlowPath"],
                                ObjectID_3, ObjectGUID_3, row["TargetObjectType"], row["TargetColumn"],
                                row["TargetDataType"], row["TargetLayerName"],
                                row['TargetSchema'], row['TargetDB'], row['TargetTableName'], row["TargetIsObjectData"],
                                1, 0, 1, str(pd.datetime.datetime.now()),'DATABASE'
                                ]

                listOfParams4 = [row["ConnectionID"], row["ConnLogicName"], row["ContainerToolType"],
                                row["ContainerObjectName"],
                                row["ContainerObjectPath"], row["ContainerObjectType"], row["ControlFlowName"],
                                row["ControlFlowPath"],
                                ObjectID_4, ObjectGUID_4, row["TargetObjectType"], row["TargetColumn"],
                                row["TargetDataType"], row["TargetLayerName"],
                                row['TargetSchema'], row['TargetDB'], row['TargetTableName'], row["TargetIsObjectData"],
                                0, 1, 1, str(pd.datetime.datetime.now()),'ETL'
                                ]

                if(row["SourceIsObjectData"]=="1" and  row["TargetIsObjectData"]=="1"):
                    print("4 vertex ")
                    #command=self.CreateStatemt(listOfParams1)
                    rid1=self.CheckInDictionary(ObjectGUID_1,listOfParams1)

                    #command = self.CreateStatemt(listOfParams2)
                    rid2 = self.CheckInDictionary(ObjectGUID_2, listOfParams2)

                    #command = self.CreateStatemt(listOfParams3)
                    rid3 = self.CheckInDictionary(ObjectGUID_3, listOfParams3)

                    #command = self.CreateStatemt(listOfParams4)
                    rid4 = self.CheckInDictionary(ObjectGUID_4, listOfParams4)

                    #CREATE EDGE DATAFLOW FROM #10:3 TO #11:4
                    # command=f'CREATE DATAFLOW FROM {rid1} TO {rid2}'
                    # self.RunApiPost(command, "POST")
                    edgeId=self.CheckInDictionaryLinks(rid1,rid2)
                    print(f'{edgeId} created!')

                    edgeId = self.CheckInDictionaryLinks(rid2, rid3)
                    print(f'{edgeId} created!')
                    edgeId = self.CheckInDictionaryLinks(rid3, rid4)
                    print(f'{edgeId} created!')

                if (row["SourceIsObjectData"] == "1" and row["TargetIsObjectData"] =="0"):
                    print("3 vertex ")



                    rid1 = self.CheckInDictionary(ObjectGUID_1, listOfParams1)

                    #command = self.CreateStatemt(listOfParams2)
                    rid2 = self.CheckInDictionary(ObjectGUID_2, listOfParams2)

                    #command = self.CreateStatemt(listOfParams4)
                    listOfParams4[20] = 0
                    rid4 = self.CheckInDictionary(ObjectGUID_4, listOfParams4)

                    # CREATE EDGE DATAFLOW FROM #10:3 TO #11:4
                    edgeId = self.CheckInDictionaryLinks(rid1, rid2)
                    print(f'Edge {edgeId} created!')
                    edgeId = self.CheckInDictionaryLinks(rid2, rid4)
                    print(f'Edge {edgeId} created!')




                if (row["SourceIsObjectData"] == "0" and row["TargetIsObjectData"] =="1"):

                    print("3 vertex ")

                    #command = self.CreateStatemt(listOfParams2)
                    listOfParams2[20] = 0
                    rid2 = self.CheckInDictionary(ObjectGUID_2, listOfParams2)

                    #command = self.CreateStatemt(listOfParams3)
                    rid3 = self.CheckInDictionary(ObjectGUID_3, listOfParams3)

                   # command = self.CreateStatemt(listOfParams4)
                    rid4 = self.CheckInDictionary(ObjectGUID_4, listOfParams4)

                    # CREATE EDGE DATAFLOW FROM #10:3 TO #11:4
                    edgeId = self.CheckInDictionaryLinks(rid2, rid3)
                    print(f'{edgeId} created!')
                    edgeId = self.CheckInDictionaryLinks(rid3, rid4)
                    print(f'{edgeId} created!')



                if (row["SourceIsObjectData"] == "0" and row["TargetIsObjectData"] =="0"):
                    print("2 vertex")

                    #command = self.CreateStatemt(listOfParams2)
                    listOfParams2[20]=0
                    rid2 = self.CheckInDictionary(ObjectGUID_2, listOfParams2)
                    listOfParams4[20] = 0
                    #command = self.CreateStatemt(listOfParams4)
                    rid4 = self.CheckInDictionary(ObjectGUID_4, listOfParams4)

                    # CREATE EDGE DATAFLOW FROM #10:3 TO #11:4
                    edgeId = self.CheckInDictionaryLinks(rid2 ,rid4)
                    print(f'{edgeId} created!')

            except Exception as e:
                   print(e)



    def CreateStatemt(self,listOfParams):
        cmdDict1 = {'ConnectionID': listOfParams[0],
                    'ConnLogicName': listOfParams[1],
                    'ToolName': listOfParams[2],
                    'ToolType': listOfParams[22],
                    'DisplayConnectionID':listOfParams[0],
                    'containerObjectName': listOfParams[3],
                    'containerObjectPath': listOfParams[4],
                    'containerObjectType': listOfParams[5],
                    'controlFlowName': listOfParams[6],
                    'controlFlowPath': listOfParams[7],
                    'ObjectID': listOfParams[8],
                    'ObjectGUID': listOfParams[9],
                    'ObjectType': listOfParams[10],
                    'ObjectName':listOfParams[11],
                    'DataType':listOfParams[12],
                    'Precision': '',
                    'Scale': '',
                    'LayerName': listOfParams[13],
                    'SchemaName': listOfParams[14],
                    'DatabaseName': listOfParams[15],
                    'TableName': listOfParams[16],
                    'IsObjectData': listOfParams[18],
                    'IsMap': listOfParams[19],
                    'IsVisible': listOfParams[20],
                    'UpdatedDate': listOfParams[21]
                    }

        txt=str(cmdDict1)
        command = "CREATE VERTEX COLUMN CONTENT " +txt
        print(command)
        return command

    def CheckInDictionary(self,ObjectGUID,listParams):
        rid="0"
        try :
            if not ObjectGUID in self.dictRids:
                command = self.CreateStatemt(listParams)
                rid = self.RunApiPost(command, "POST")
                self.dictRids[ObjectGUID] = rid
                with open(self.dictFileName, "w") as outfile:
                    outfile.write(json.dumps(self.dictRids))
            else:
                rid = self.dictRids[ObjectGUID]
        except Exception as e:
            print(e)
        return  rid


    def CheckInDictionaryLinks(self,fromRid,toRid):

        edgeId="0"
        try:
            if  fromRid  in  self.dictEdges:
                if self.dictEdges[fromRid] != toRid:
                    command = f'CREATE EDGE DATAFLOW FROM {fromRid} TO {toRid}'
                    edgeId=self.RunApiPost(command, "POST")
                    self.dictEdges[fromRid]=toRid
                    with open(self.dictFileNameEdges, "w") as outfile:
                        outfile.write(json.dumps(self.dictEdges))
                    return edgeId
                else:
                    return  edgeId
            else:
                command = f'CREATE EDGE DATAFLOW FROM {fromRid} TO {toRid}'
                edgeId = self.RunApiPost(command, "POST")
                self.dictEdges[fromRid] = toRid
                with open(self.dictFileNameEdges, "w") as outfile:
                    outfile.write(json.dumps(self.dictEdges))
                return edgeId

        except Exception as e:
            print(e)











