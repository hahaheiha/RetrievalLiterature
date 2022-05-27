import os

from elasticsearch import Elasticsearch

#ES数据库的用户名及密码
user = 'elastic'
password = 'WG*_MuA9FxQwizPP=hIl'

tableName = 'retrieval_literature'
#创建ES数据库, 由于使用的是本地连接故此处填入：127.0.0.1
es = Elasticsearch(['127.0.0.1'], http_auth=(user, password), timeout=3600)
#创建一张新的表命名为: retrieval_literature
es.indices.create(index=tableName)


#得到文献数据的路径
dataPath = "./RetrievalData"
#得到文件夹中的所有文件名称
fList = os.listdir(dataPath)

#遍历文件夹中的所有文件中的数据，并把加入到ES数据库中
#数据庞大该过程将执行很长一段时间
for fileName in fList:
    filePath = dataPath + "/" + fileName
    #当文献为GBK编码时使用改方法打开
    with open(filePath, mode='r', encoding='GBK', errors='ignore') as f:
    # with open(filePath, mode='r', encoding='utf8', errors='ignore') as f:
        

        dataMap = {}
        totalLine = ''
        while True:
            line = f.readline()
            if line == '<REC>\n':
                if dataMap.__len__() != 0:
                    splitLine = totalLine.split("=")
                    dataMap.__setitem__(splitLine[0].replace('<', '').replace('>', ''), splitLine[1])
                    totalLine = ''
                    es.index(index=tableName, doc_type='_doc', body=dataMap)
                    dataMap.clear()
            else:
                if line.startswith('<'):
                    if totalLine.__contains__(">="):
                        splitLine = totalLine.split("=")
                        dataMap.__setitem__(splitLine[0].replace('<', '').replace('>', ''), splitLine[1])
                    totalLine = line
                else:
                    totalLine = totalLine + line
            if line == '':
                break


        if dataMap.__len__() != 0:
            splitLine = totalLine.split("=")
            dataMap.__setitem__(splitLine[0].replace('<', '').replace('>', ''), splitLine[1])
            es.index(index=tableName, doc_type='_doc', body=dataMap)
            dataMap.clear()
print("建立索引成功！")