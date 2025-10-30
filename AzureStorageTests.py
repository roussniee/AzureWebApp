import os
import yaml
import pandas as pd
from datetime import datetime, timedelta
from azure.storage.blob import ContainerClient, BlobServiceClient, generate_blob_sas, BlobSasPermissions #to manipulate blob container and its blobs

#credentials
dev_account_name = 'dev_account_name'
dev_account_key = 'dev_account_key'
container1_name = 'container1'
container2_name = 'container2'

#client to interat w blob storage
dev_connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + dev_account_name + ';AccountKey=' + dev_account_key + ';EndpointSuffix=core.windows.net'
dev_blob_service_client = BlobServiceClient.from_connection_string(dev_connect_str)

#use the client to connect to the containers
dev_container1_client = dev_blob_service_client.get_container_client(container1_name)
dev_container2_client = dev_blob_service_client.get_container_client(container2_name)

#get a list of all blob files in the containers (none rn)
blob_list = []
for blob_i in dev_container1_client.list_blobs():
    blob_list.append(blob_i.name)


