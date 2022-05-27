from elasticsearch import Elasticsearch

es = Elasticsearch(['127.0.0.1'], http_auth=('elastic', 'WG*_MuA9FxQwizPP=hIl'), timeout=3600)

def getLikeSearch(like):

    splitLike = like.split(':')

    body = {

        'from': 0,
        'query': {
            # 查询命令
            'match': {
                # 查询方法：模糊查询
                # '引文': '《青海大通长宁遗址》'  # content为字段名称，match这个查询方法只支持查找一个字段
                splitLike[0]: splitLike[1]  # content为字段名称，match这个查询方法只支持查找一个字段
            }
        }
    }

    result = es.search(index='retrieval_literature', body=body)
    # result = es.search(index='retrieval_literature', filter_path=filter_path, body=body)
    # h.get()
    resultList = result.get('hits').get('hits')

    return resultList

def getPreciousSearch(pre):
    splitPre = pre.split(':')
    li = splitPre[1].split(',')
    # 精确单值查询
    body = {

        "query": {

            "terms": {

                 splitPre[0] + ".keyword" : li  # 查询keyword="食物"或"测试"...的数据
                #'第一机构': '四川大学考古学系;\n'
            }
        }
    }

    result = es.search(index='retrieval_literature', body=body)
    # result = es.search(index='retrieval_literature', filter_path=filter_path, body=body)
    # h.get()
    resultList = result.get('hits').get('hits')

    return resultList

# filter_path = ['hits.hits._source._score'
#                 'hits.hits._source.篇名',
#                'hits.hits._source.引文']  # 字段2
# print(es.search(index='es_zilongtest'))

# print(resultList)

def search(content, mode = 1):
    if mode == 1:
        return getLikeSearch(content)
    elif mode == 2:
        return getPreciousSearch(content)