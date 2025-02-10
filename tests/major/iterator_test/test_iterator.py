import os
import time
import random
import string
import numpy as np
import pandas as pd
from pymilvus import (
    connections,
    utility,
    FieldSchema, CollectionSchema, DataType,
    Collection,
)

from loguru import logger
import os
import struct
import sys

path = "/home/hanchun/temp"
log_path = path + "/log" + "/iterator.log"
logger.add(log_path)

fmt = "\n=== {:30} ===\n"
dim = 768

logger.info(fmt.format("start connecting to Milvus"))
connections.connect(uri="https://in01-e599d4e68af2c7d.az-eastus.vectordb.zillizcloud.com:19530", user="zcloud_root", password="s9.,|&xV^D{eN]C27{-6J/qU*1C,-o~r")



# 1. create collection
collection_name = "LM_transcript_rag"
collection = Collection(name=collection_name)
res = collection.query(expr="", output_fields=["count(*)"])
#res = collection.query(expr='id != \"\"')
logger.info(res)

