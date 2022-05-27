# f = open("CJFDLASN2018.txt","r", encoding="GBK")
# line = f.readline()
# print(line)
import os

from elasticsearch import Elasticsearch

# es = Elasticsearch()

es = Elasticsearch(['127.0.0.1'], http_auth=('elastic', 'WG*_MuA9FxQwizPP=hIl'), timeout=3600)

dataPath = "./RetrievalData"


fList = os.listdir(dataPath)

# dataMap = {}


for fileName in fList:
    filePath = dataPath + "/" + fileName
    with open(filePath, mode='r', encoding='utf8', errors='ignore') as f:
        dataMap = {}
        cData = []
        infoData = []

        # print(sqlExe)
        totalLine = ''
        while True:
            line = f.readline()
            if line == '<REC>\n':
                if dataMap.__len__() != 0:
                    for k, v in dataMap.items():
                        # cData.append(k)
                        # infoData.append(v)
                        es.index(index='test_wp', doc_type='_doc', body=dataMap)
                        print(k, v)
                    # cursor.execute(sqlExe)
                    dataMap.clear()
                    # cData.clear()
                    # infoData.clear()
                    break

                # print(sqlExe)
            if line == '':
                break

            if line.startswith('<'):
                if totalLine.__contains__(">="):
                    splitLine = totalLine.split("=")
                    # if splitLine[1] != '\n':
                    dataMap.__setitem__(splitLine[0].replace('<', '').replace('>', ''), splitLine[1])
                totalLine = line
                    # else:
                    #     dataMap.__setitem__(splitLine[0].replace('<', '').replace('>', ''), 'CRLF')

            else:
                totalLine = totalLine + line


# es.indices.create(index="es_zilongtest")
# es.indices.create(index="es_zilongtest")

# body={'keyword':'测试',"content":"这是一个测试数据1"}
# es.index(index='es_test',doc_type='_doc',body=body)

# doc={'keyword':'食物', "content":"我喜欢吃大白菜"}
# doc={'keyword':'不知', "content":"我喜欢吃", '测试':'看看'}
# es.index(index='es_zilongtest',doc_type='_doc',body=doc)


print(es.search(index='test_wp'))

# es = Elasticsearch(
#     [{"host":"xx.xx.xx.xx","port":"9200"}],
#     scheme="https",
#     # ssl_context=ctx,
# )

# conn = Elasticsearch([{'host': "127.0.0.1", 'port': "9200", "http_auth": "%s:%s" % ("elastic", "BCyLU9YWs6bJMtQ-eumo")}])

# HOST = '127.0.0.1:9200' # es数据库ip
# es = Elasticsearch([HOST])

# es.indices.create(index='es_python', ignore=400)
# body = {'name':'刘婵',"age":6,
# 		"sex":"male",'birthday':'1984-01-01',
# 		"salary":-12000}
# es.index(index='es_python',doc_type='_doc',body=body)
#
#
# es.search(index='es_python')