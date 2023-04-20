import jiagu
from kafka import KafkaConsumer
import json
import pymysql.cursors
# 语言处理消费者


consumer = KafkaConsumer(
    'weibo_source',
    bootstrap_servers=['43.143.23.30:9092'],
    group_id='my-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
api_version=(0,10,1)
)
# mysql连接
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='123456',
                             database='weibo',
                             cursorclass=pymysql.cursors.DictCursor)

with connection:
    with connection.cursor() as cursor:
       for message in consumer:
         data = message.value.decode('utf-8')
         print(data)
         # print(f"Received message: {data}")
         weibo =json.loads(data)
         # 抽取文本部分
         text = weibo['text']
         sentiment = jiagu.sentiment(text)
         # 分析情感
         weibo['sentiment'] = sentiment[0]
          # 分析关键字
         keywords = jiagu.keywords(text, 10)
         # 提取监控字段的关键字
         duplicated_keyword = jiagu.keywords(weibo['monitoring'])
          # 去重监控字段的关键字 避免没用的关键字出现在词云内
         keywords_save = list(filter(lambda k: k not in duplicated_keyword, keywords))
         weibo['keywords'] = ",".join(keywords_save)
         # print(weibo)
         # Create a new record
         keys = ', '.join(weibo.keys())
         v =  ["'{}'".format(x) for x in weibo.values()]
         values = ', '.join(v)

         sql = """INSERT INTO {table}({keys}) VALUES ({values})""".format(table='weibo',
                                                                          keys=keys,
                                                                          values=values)
         print(sql)
         try:
             cursor.execute(sql)
             connection.commit()
         except Exception as e:
             print('Error',e)
#     落盘mysql








