import sys
import pandas as pd
import datetime
import requests
import happybase
import json
import ast
import time
from dotenv import load_dotenv

from kafka import KafkaProducer,KafkaConsumer
from json import dumps
import os
from datetime import date
from json import loads

load_dotenv()

from datetime import datetime


kafka_servers = os.environ["KAFKA_HOST_URL"]
influencer_history_tbl = os.environ['INFLUECNER_HISTORY_TBL']
influencer_tbl = os.environ['INFLUENCER_TBL']
twitter_topic = os.environ["TWITTER_SM_TOPIC"]
insta_topic = os.environ["INSTA_SM_TOPIC"]
youtube_topic = os.environ["YOUTUBE_SM_TOPIC"]
host = os.environ["HBASE_HOST"]
port = int(os.environ["HBASE_PORT"])

class scheduler():
    def __init__(self):
        print("init called")

    def kakfaProducer(self, topicName, tabledata):
        try:
            print("sending kafka data")
            kafkaProducer = KafkaProducer(bootstrap_servers=kafka_servers,value_serializer=lambda x: dumps(x).encode('utf-8'))
            kafkaProducer.send(topicName, value=tabledata)
            kafkaProducer.flush()

        except Exception as e:
            e.__str__()
            print("error in sending kafka", e)


    def getfilteredInfluencerData(self,tableName,influencerId,current_date):
        try:
            connection = happybase.Connection(host=host, port=port,autoconnect=True,transport='framed',protocol='compact')
            if tableName == influencer_history_tbl:
                print("IN ",tableName)
                print("current_date",current_date)
                current_date_time = str(datetime.now().date())
                csfilter = "SingleColumnValueFilter('{column_family}_data','nextRunDate',=,'binary:{nextRunDate}')".format(column_family =tableName,nextRunDate = current_date_time)
                print(csfilter)

            elif tableName == influencer_tbl:
                print("IN ",tableName)
                csfilter = "SingleColumnValueFilter('{column_family}_data','influencerId',=,'binary:{influencerId}')".format(column_family =tableName,influencerId = influencerId)
                print(csfilter)

            entire_table = []
            table = connection.table(tableName)
            for key,data in table.scan(filter = csfilter):
                data_key=key
                hbase_dict = data
                values = hbase_dict.values()
                hbase_values = list(values)
                entire_table.append(hbase_values)
            table_list = []
            for x in range(len(entire_table)):
                hbase_values_for_decoding=entire_table[x]
                hbase_values_decoded = []
                for y in range(len(hbase_values_for_decoding)):
                    hbase_values_decoded.append(hbase_values_for_decoding[y].decode("utf-8"))
                table_list.append(hbase_values_decoded)
                rows = table.rows([data_key])
                for key, data in rows:
                    hbase_dict = data
                    keys = hbase_dict.keys()
                    hbase_keys = list(keys)
                    hbase_keys_decoded = []
                    for x in range(len(hbase_keys)):
                        hbase_keys_decoded.append(hbase_keys[x].decode("utf-8").split(':')[1])
            df = pd.DataFrame(table_list,columns=hbase_keys_decoded)
            connection.close()        
            return df
            
        except Exception as e:
            print(e,"exception in getInfluencerData")
            df = pd.DataFrame()
            return df

    def main(self):
        historytblDF = self.getfilteredInfluencerData(influencer_history_tbl,"",str(datetime.now().date()))
        if not historytblDF.empty:
            for i,data in historytblDF.iterrows():
                inf_id = data["influencerId"]
                print("running scheduler for the influencerid..",inf_id)
                social_handle = data["socialProfile"]
                reques_type = data["requestType"]
                fileteredDF = self.getfilteredInfluencerData(influencer_tbl,inf_id,str(datetime.now().date()))
                if not fileteredDF.empty:
                    total_data = {}
                    data_dict = fileteredDF.to_dict('list')
                    total_data["category"] = data_dict["categories"][0]
                    total_data["country"] = data_dict["country"][0]
                    total_data["email"] = data_dict["email"][0]
                    total_data["influencerId"] = data_dict["influencerId"][0]
                    if  isinstance(social_handle, str):
                        social_handle = ast.literal_eval(social_handle)
                        
                    total_data["social_handle"] = social_handle
                    total_data["Requesttype"] = int(reques_type)

                    if  isinstance(total_data, str):
                        total_data = ast.literal_eval(total_data)

                    if "Twitter" in social_handle.keys():
                        self.kakfaProducer(twitter_topic, total_data)
                    elif "Instagram" in social_handle.keys():
                        self.kakfaProducer(insta_topic, total_data)
                    elif "Youtube" in social_handle.keys():
                        self.kakfaProducer(youtube_topic, total_data)
                else:
                    print("No matching influencerId in influencer table..")
        else:
            print(historytblDF,"got empty DF")

if __name__ == "__main__":
    print('start of main method...')
    inf_obj = scheduler()
    inf_obj.main()
    print('End of main method....')
    
