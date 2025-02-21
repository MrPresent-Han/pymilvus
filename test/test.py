from pymilvus import connections
from pymilvus.orm import utility
from pymilvus import Collection
from datetime import datetime

import time

if __name__ == '__main__':
    LOG_LEVEL = "DEBUG"
    connections.connect(uri="https://in01-082bf9a903ac692.ali-cn-shenzhen.vectordb.zilliz.com.cn:19530",
                        token="reader:db_Reader")
    col = Collection("dreams")
    iterator = col.query_iterator(expr="pk like 'nfs://%'", batch_size=10000, output_fields=["pk", "embeddings"], reduce_stop_for_best=False)
    # iterator = col.query_iterator(
    #     batch_size=1,
    #     # expr="party_id == '1725369385622265857' and party_type == 'TEAM_KNOWLEDGE' and tntid in ['1724007765359353857', '9999'] and bizid == '0000' and code in ['1803395339810660354','1803395339810660355','1803395339810660356','1803395339810660357','1803395339814854657','1803395339814854658','1803395339814854659','1803395339814854660','1803395339814854661','1803395339814854662','1803395339814854663','1803395339814854664','1803395339814854665','1803395339814854666','1803395339814854667','1803395339819048961','1803395339819048962','1803395339819048963','1803395339823243265','1803395339823243266','1803395339823243267','1803395339823243268','1803395339823243269','1803395339823243270','1803395339823243271','1803395339827437569','1803395339827437570'] and type in ['QA', 'TEXT']",
    #     expr="id > 0",
    #     output_fields=["id", "title_vector"]
    # )
    total = 0
    
    results = []
    while True:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Begin: {current_time} count: {total}")
        result = iterator.next()
        if not result:
            iterator.close()
            break
        
        total += len(result)
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Done: {current_time} count: {total}")
        # results += result
        # print(result)
        # time.sleep(3000)

