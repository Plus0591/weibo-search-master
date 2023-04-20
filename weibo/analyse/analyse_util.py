from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, desc, col, date_format, count, when

mysql_user = "root"
mysql_password = "123456"
mysql_url = "jdbc:mysql://192.168.10.1:3306/weibo?useSSL=false&serverTimezone=UTC"

# 帮我写一个pyspark代码，user_id字段代表某个人，sentiment代表他发表的本条评论的情绪，统计出发表的评论情绪值为negative的次数占他自己所有评论比重超过80%的并且发表评论数大于10条的人，并且显示它的user_id,screen_name字段，显示他发表的负面评论统计次数，和他发表的所有评论的统计次数
def analyse_unusual(df):
    # 计算每个用户的评论总数和负面评论数量
    user_sentiment_counts = df.groupBy("user_id").agg(
        count("sentiment").alias("total_comments"),
        count(when(col("sentiment") == "negative", True)).alias("negative_comments")
    )
    # 计算负面评论比例
    user_sentiment_ratios = user_sentiment_counts.withColumn(
        "negative_ratio", col("negative_comments") / col("total_comments")
    )
    # 过滤出负面评论比例超过 80% 的用户
    selected_users = user_sentiment_ratios.filter(col("negative_ratio") > 0.8)
    # 选取需要的列，并显示结果
    result = selected_users.join(df.select("user_id", "screen_name"), "user_id").select(
        "user_id", "screen_name", "negative_comments", "total_comments"
    ).distinct()
    result.show()

def analyse_date(df):
    df = df.withColumn("created_at", date_format("created_at", "yyyy-MM-dd"))
    # 按照日期分组并计数
    counts = df.groupBy("created_at").agg(count("*").alias("count"))
    for i, row in enumerate(counts.collect()):
        number = row["count"]
        print(f"Row {i + 1}: {row.created_at}{number}")


def analyse_sentiment(df):
    negative_rows = df.filter(df.sentiment == 'negative')
    positive_rows = df.filter(df.sentiment == 'positive')
    total_number = df.count()
    negative_count = negative_rows.count()
    positive_count = positive_rows.count()
    print(f"The number of total sentiments is: {total_number}")
    print(f"The number of positive sentiments is: {positive_count}")
    print(f"The number of negative sentiments is: {negative_count}")


def analyse_keywords(df):
    df = df.select(split("keywords", ",").alias("keywords_array"))
    # 使用explode函数将数组转换为多行数据
    df = df.select(explode("keywords_array").alias("keyword"))
    # 计算每个关键字的出现频率
    df = df.groupBy("keyword").count()
    # 按照出现频率降序排列
    df = df.sort("count", ascending=False).limit(10)
    # 显示出现频率最高的关键字及其频率
    for i, row in enumerate(df.collect()):
        number = row["count"]
        print(f"Row {i + 1}: {row.keyword}{number}")


def analyse_hot(df):
    attitude_max = df.orderBy(desc("attitudes_count")).limit(10)
    comments_max = df.orderBy(desc("comments_count")).limit(10)
    reposts_max = df.orderBy(desc("reposts_count")).limit(10)
    for i, row in enumerate(attitude_max.collect()):
        value = row.text  # 将 xx 替换为您要获取的列名
        print(f"Row {i + 1}: {value}")


if __name__ == "__main__":
    # 创建一个SparkSession对象
    spark = SparkSession.builder \
        .appName("Analyse") \
        .master("local[*]") \
        .getOrCreate()
    jdbc_url = mysql_url
    connection_properties = {
        "user": mysql_user,
        "password": mysql_password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df = spark.read.jdbc(url=jdbc_url, table='weibo', properties=connection_properties)
    start_time = "2022-12-01"
    end_time = "2022-12-10"
    monitoring_value = "北京大学"
    df = df.filter((col("created_at").between(start_time, end_time)) & (col("monitoring") == monitoring_value))
    analyse_unusual(df)
    spark.stop()
