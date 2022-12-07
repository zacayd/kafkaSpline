
import  pandas as pd
import numpy as np
from tabulate import tabulate
import requests
import json
import  datetime
from datetime import datetime
from kafka import KafkaConsumer

from OrientApi import OrientDBApi

topic1 = 'spline-topic'
brokers = ['192.168.100.11:9092']



class Consumer:
 def __init__(self,topicName, consumerUrl, arangoDBUrl,group_id,brokers,orientDB):
    self.arangoDBUrl = arangoDBUrl
    self.topicName=topicName
    self.brokers=brokers
    self.consumerUrl=consumerUrl
    self.orientDB=orientDB
    self.consumer = KafkaConsumer(topicName,bootstrap_servers=brokers,group_id=group_id)

 def GetMessagesFromTopic(self):
     for message in self.consumer:
         listOfMessages = json.loads(message.value)

         listOfExectionPlans=[x["_key"] for x in listOfMessages]
         print(listOfExectionPlans)
         for exc in listOfExectionPlans:
             print("Start..")
             print(datetime.now())

             txt=self.GetLineageDetailedForExecPlan(exc)
             #print("DataTypes:")
             dfDataTypes=self.WriteJsonToDataFrame(txt,'executionPlan',['extra', 'dataTypes'],['_id', 'name'])
             #print("attributes:")
             dfAttributes=self.WriteJsonToDataFrame(txt, 'executionPlan', ['extra', 'attributes'], ['_id', 'name'])
             #print("inputs:")
             dfInputs=self.WriteJsonToDataFrame(txt, 'executionPlan', ['inputs'], ['_id', 'name'])
             #print("nodes:")
             dfNodes=self.WriteJsonToDataFrame(txt, 'graph', ['nodes'], None)
             #print("edges:")
             dfEdges=self.WriteJsonToDataFrame(txt, 'graph', ['edges'], None)
             #print("readsFrom:")
             txt=self.GetDocsFromCollection(exc,'readsFrom')
             dfReadsFrom =self.WriteJsonToDataFrame(txt, None, None, None)
             #print("writesTo:")
             txt = self.GetDocsFromCollection(exc,'writesTo')
             dfWritesTo= self.WriteJsonToDataFrame(txt, None, None, None)
             #print("dataSources:")
             txt=self.GetAllDataSources()
             dfDataSource=self.WriteJsonToDataFrame(txt, None, None, None)

             self.CreateSourceToTargetTable(exc,dfDataTypes,dfAttributes,dfInputs,dfNodes,dfEdges,dfReadsFrom,dfWritesTo,dfDataSource)
             print("End")
             print(datetime.now())


 def CreateSourceToTargetTable(self,excId, dfDataTypes,dfAttributes,dfInputs,dfNodes,dfEdges,dfReadsFrom,dfWritesTo,dfDataSource):
     print("Create join...")
     result = pd.merge(dfEdges, dfNodes, left_on='xsource', right_on='x_id', how='left')[['xsource','xtarget','xname','x_type']]


     result.rename(columns={'xsource': 'Source_id', 'xtarget': 'Target_id','xname':'sourceLayerName','x_type':'SourceObjectType'}, inplace=True)

     result = pd.merge(result, dfNodes, left_on='Target_id', right_on='x_id', how='left')[
         ['Source_id', 'Target_id','sourceLayerName','SourceObjectType', 'xname', 'x_type']]

     result.rename(columns={'xname': 'TargetLayerName', 'x_type': 'TargetObjectType'}, inplace=True)


     result["Source_id_op"] = 'operation/'+result["Source_id"].astype(str)
     result["Target_id_op"] = 'operation/' + result["Target_id"].astype(str)



     result = pd.merge(result, dfReadsFrom, left_on='Source_id_op', right_on='_from', how='left')[['Source_id', 'sourceLayerName',  'SourceObjectType', 'Target_id','Source_id_op','Target_id_op',
          'TargetLayerName','TargetObjectType','_to']].rename(columns={'_to': 'sourceDatasourceID'})


     #
     # print(tabulate(result, headers='keys', tablefmt='psql'))
     #
     result = pd.merge(result, dfDataSource, left_on='sourceDatasourceID', right_on='_id', how='left')[['Source_id', 'sourceLayerName', 'SourceObjectType', 'Target_id', 'TargetLayerName','TargetObjectType','uri','Source_id_op','Target_id_op']].rename(columns={'uri': 'SourceTableName'})

     result=pd.merge(result, dfWritesTo, left_on='Target_id_op', right_on='_from', how='left')[['Source_id', 'sourceLayerName', 'SourceObjectType', 'Target_id',
          'TargetLayerName','TargetObjectType','SourceTableName','_to']].rename(columns={'_to': 'targetDatasourceID'})



     result = pd.merge(result, dfDataSource, left_on='targetDatasourceID', right_on='_id', how='left')[
         ['Source_id', 'sourceLayerName', 'SourceObjectType','SourceTableName', 'Target_id',
          'TargetLayerName', 'TargetObjectType', 'uri']].rename(
         columns={'uri': 'TargetTableName'})



     result['ControlFlowPath'] = excId



     result = pd.merge(result, dfAttributes, left_on='ControlFlowPath', right_on='_id', how='left')
     result=result[['ControlFlowPath','Source_id','sourceLayerName','SourceObjectType','SourceTableName','Target_id','TargetLayerName','TargetObjectType','TargetTableName','xname','xid','xdataTypeId']].rename(columns={'xname': 'SourceColumn','xid':'SourceColumnID'})



     result=pd.merge(result, dfDataTypes, left_on='xdataTypeId', right_on='xid', how='left')

     result = result[
         ['ControlFlowPath', 'Source_id', 'sourceLayerName', 'SourceObjectType', 'SourceTableName','SourceColumnID','SourceColumn','xname', 'Target_id',
          'TargetLayerName', 'TargetObjectType', 'TargetTableName']].rename(
         columns={'xname': 'SourceDataType'})



     result['TargetColumnID']=result['SourceColumnID'].astype(str)
     result['TargetColumn'] = result['SourceColumn'].astype(str)
     result['TargetDataType'] = result['SourceDataType'].astype(str)
     result['SourceObjectType']=result['sourceLayerName'].astype(str)
     result['TargetObjectType'] = result['TargetLayerName'].astype(str)

     #print(str(result["SourceTableName"].astype(str)).split('/')[-1])


     result["SourceTableName"] = np.where(result["SourceTableName"] == "nan", '', result["SourceTableName"])

     result["TargetTableName"] = np.where(result["TargetTableName"] == "nan", '', result["TargetTableName"])


     result['SourceOp'] = result['Source_id'].apply(lambda x: x.split(':')[-1])

     result['sourceLayerName'] = result['sourceLayerName'] +'_'+result['SourceOp']

     result['TargetOp'] = result['Target_id'].apply(lambda x: x.split(':')[-1])



     result['TargetLayerName'] = result['TargetLayerName'] + '_' + result['TargetOp']

     result['SourceIsObjectData'] =  np.where(result["SourceObjectType"] == "Project", '0','1')
     result['TargetIsObjectData'] = np.where(result["TargetObjectType"] == "Project", '0', '1')

     result['SourceSchema'] = result['SourceTableName'].apply(lambda x: '-1' if '.csv' in str(x)  or 'dbfs' in str(x) else 'Schema')
     result['SourceDB'] = result['SourceTableName'].apply(lambda x: '-1' if '.csv' in str(x) or 'dbfs' in str(x) else 'DB')
     result['TargetSchema'] = result['TargetTableName'].apply(lambda x: '-1' if '.csv' in str(x)  or 'dbfs' in str(x) else 'Schema')
     result['TargetDB'] = result['TargetTableName'].apply(lambda x: '-1' if '.csv' in str(x)  or 'dbfs' in str(x) else 'DB')


     result["SourceSchema"] = np.where(result["SourceIsObjectData"] == "0", '', result["SourceSchema"])
     result["SourceDB"] = np.where(result["SourceIsObjectData"] == "0", '', result["SourceSchema"])
     result["SourceTableName"] = np.where(result["SourceIsObjectData"] == "0", '', result["SourceTableName"])

     result["TargetSchema"] = np.where(result["TargetIsObjectData"] == "0", '', result["TargetSchema"])
     result["TargetDB"] = np.where(result["TargetIsObjectData"] == "0", '', result["TargetDB"])
     result["TargetTableName"] = np.where(result["TargetIsObjectData"] == "0", '', result["TargetTableName"])
     result["ConnectionID"]="200" #from topic conn xml
     result["ConnLogicName"] = "Databricks" #from topic conn xml
     result["ContainerToolType"] = "Databricks"
     result["SourceServer"]=""
     result["TargetServer"] = ""
     result["ContainerObjectPath"]=excId
     result["ContainerObjectName"]=excId
     result["ContainerObjectType"] = 'Notebook'
     result["ContainerToolType"] = 'Databricks'

     result["ControlFlowName"] = excId
     result['ControlFlowPath'] = excId

     result = result[
         ['ConnectionID','ConnLogicName', 'ContainerObjectName','ContainerObjectPath','ContainerObjectType','ContainerToolType','ControlFlowName','ControlFlowPath',
          'Source_id','sourceLayerName','SourceSchema','SourceDB','SourceTableName','SourceColumn','SourceColumnID','SourceDataType', 'SourceObjectType',
          'Target_id','TargetLayerName','TargetSchema','TargetDB','TargetTableName','TargetColumn','TargetColumnID','TargetDataType','TargetObjectType',
          'SourceIsObjectData','TargetIsObjectData','SourceServer','TargetServer']]


     print("Result:")
     print(tabulate(result, headers='keys', tablefmt='psql'))






     orientApi = OrientDBApi('http://192.168.100.11:2480', "Databricks_E2E", "vertex.json", "edges.json")
     orientApi.CreateVertices(result)






     #result = pd.merge(result, dfReadsFrom, left_on='Source_id', right_on='_from', how='left')[['_to','Source_id','sourceLayerName','SourceObjectType','Target_id','TargetLayerName','TargetObjectType']]

     #[['Source_id','sourceLayerName','SourceObjectType','Target_id','TargetLayerName','TargetObjectType']


     #df4 = pd.merge(pd.merge(dfEdges, dfNodes, on='Courses'), df3, on='Courses')







# print the value of the consumer
# we run the consumer generator to fetch the message scoming from topic1.

 def WriteJsonToDataFrame(self,text,dataKey,recordPath,meta):
    data=json.loads(text)

    if (dataKey!=None):
        result = pd.json_normalize(data[dataKey], record_path=recordPath, meta=meta,
                               record_prefix='x', errors='ignore')
    else:
        result = pd.json_normalize(data)

    #print(tabulate(result, headers='keys', tablefmt='psql'))
    return   result


 def GetAllDataSources(self):

     url = self.arangoDBUrl

     payload = json.dumps({
         "query": "FOR u IN dataSource RETURN u"
     })
     headers = {
         'accept': 'application/json',
         'Content-Type': 'application/json'
     }

     response = requests.request("POST", url, headers=headers, data=payload)
     data = json.loads(response.text)['result']
     return json.dumps(data)


 def GetDocsFromCollection(self,execplanID,collectionName):
     #url = "http://192.168.100.11:8529/_db/spline/_api/cursor"
     url=self.arangoDBUrl

     payload = json.dumps({
         "query": "FOR u IN {collectionName} FILTER u._belongsTo=='executionPlan/{execplanID}' RETURN u".replace("{execplanID}",execplanID).replace("{collectionName}",collectionName)
     })
     headers = {
         'accept': 'application/json',
         'Content-Type': 'application/json'
     }

     response = requests.request("POST", url, headers=headers, data=payload)
     data= json.loads(response.text)['result']
     return json.dumps(data)

 def GetLineageDetailedForExecPlan(self,execplanID):


    url = f'{self.consumerUrl}?execId={execplanID}'

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    print(f'execution plan:"{execplanID}')
    return response.text


class ConsumerExecution:
    def __init__(self):
        self.topic1 = 'spline-topic'
        self.brokers = ['192.168.100.11:9092']
        self.consumerUrl='http://192.168.100.11:8080/consumer/lineage-detailed'
        self.arangoDBUrl = "http://192.168.100.11:8529/_db/spline/_api/cursor"
        self.orientDB="http://192.168.100.11:2480"
        self.group_id='myGroup'

        self.p1 = Consumer(self.topic1 , self.consumerUrl, self.arangoDBUrl,self.group_id,self.brokers,self.orientDB)

    def run(self):
        self.p1.GetMessagesFromTopic()




consumerEx=ConsumerExecution()
consumerEx.run()



