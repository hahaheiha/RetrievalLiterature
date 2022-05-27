from elasticsearch import Elasticsearch

es = Elasticsearch(['127.0.0.1'], http_auth=('elastic', 'WG*_MuA9FxQwizPP=hIl'), timeout=3600)

def getLikeSearch(like):

    body = {

        'from': 0,
        'query': {
            # 查询命令
            'match': {
                # 查询方法：模糊查询
                # '引文': '《青海大通长宁遗址》'  # content为字段名称，match这个查询方法只支持查找一个字段
                '引文': like  # content为字段名称，match这个查询方法只支持查找一个字段
            }
        }
    }

    result = es.search(index='retrieval_literature', body=body)
    # result = es.search(index='retrieval_literature', filter_path=filter_path, body=body)
    # h.get()
    resultList = result.get('hits').get('hits')

    return resultList

# 精确单值查询
body1 = {

    "query": {

        "terms": {

            "篇名.keyword": ["平遥古城甲天下\n"]  # 查询keyword="食物"或"测试"...的数据
            #'第一机构': '四川大学考古学系;\n'
        }
    }
}

result = es.search(index='retrieval_literature', body=body1)
print(result)
# filter_path = ['hits.hits._source._score'
#                 'hits.hits._source.篇名',
#                'hits.hits._source.引文']  # 字段2
# print(es.search(index='es_zilongtest'))

# print(resultList)