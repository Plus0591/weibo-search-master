import sys
import pymysql
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from weibo.utils.region import region_dict
from scrapy.utils.project import get_project_settings
# scrapy crawl search -a keyword=福建江夏学院 -a start_date=2023-01-01 -a end_date=2023-01-01
settings = get_project_settings()
def convert_weibo_type(weibo_type):
    """将微博类型转换成字符串"""
    if weibo_type == 0:
        return '&typeall=1'
    elif weibo_type == 1:
        return '&scope=ori'
    elif weibo_type == 2:
        return '&xsort=hot'
    elif weibo_type == 3:
        return '&atten=1'
    elif weibo_type == 4:
        return '&vip=1'
    elif weibo_type == 5:
        return '&category=4'
    elif weibo_type == 6:
        return '&viewpoint=1'
    return '&scope=ori'


def get_mysql():

    conn = pymysql.connect(host='localhost',
                           user='root',
                           password='123456',
                           database='weibo',
                           cursorclass=pymysql.cursors.DictCursor)
    return conn

MYSQL_HOST = '192.168.101.83'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWORD = '123456'
MYSQL_DATABASE = 'weibo'


def get_spark_session(appName, table):
    '''
        计算两个数字的和
        Args:
            appName (str): app名字
            table (str): 要查的表名字
        Returns:
            spark: 记得关 没啥用 spark.stop()
            df: 数据帧
        '''
    spark = SparkSession.builder.appName(appName).getOrCreate()
    url = 'jdbc:mysql://'+settings.get('MYSQL_HOST')+':'+settings.get('MYSQL_PORT')+'/'+settings.get('MYSQL_DATABASE')
    print(url)
    user = "root"
    password = "weibo"
    df = spark.read.format("jdbc").option("url", url).option("dbtable", table).option("user", user).option("password",
                                                                                                           password).load()
    return spark, df


def convert_contain_type(contain_type):
    """将包含类型转换成字符串"""
    if contain_type == 0:
        return '&suball=1'
    elif contain_type == 1:
        return '&haspic=1'
    elif contain_type == 2:
        return '&hasvideo=1'
    elif contain_type == 3:
        return '&hasmusic=1'
    elif contain_type == 4:
        return '&haslink=1'
    return '&suball=1'


def get_keyword_list(file_name):
    """获取文件中的关键词列表"""
    with open(file_name, 'rb') as f:
        try:
            lines = f.read().splitlines()
            lines = [line.decode('utf-8-sig') for line in lines]
        except UnicodeDecodeError:
            print(u'%s文件应为utf-8编码，请先将文件编码转为utf-8再运行程序', file_name)
            sys.exit()
        keyword_list = []
        for line in lines:
            if line:
                keyword_list.append(line)
    return keyword_list


def get_regions(region):
    """根据区域筛选条件返回符合要求的region"""
    new_region = {}
    if region:
        for key in region:
            if region_dict.get(key):
                new_region[key] = region_dict[key]
    if not new_region:
        new_region = region_dict
    return new_region


def standardize_date(created_at):
    """标准化微博发布时间"""
    if "刚刚" in created_at:
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    elif "秒" in created_at:
        second = created_at[:created_at.find(u"秒")]
        second = timedelta(seconds=int(second))
        created_at = (datetime.now() - second).strftime("%Y-%m-%d %H:%M")
    elif "分钟" in created_at:
        minute = created_at[:created_at.find(u"分钟")]
        minute = timedelta(minutes=int(minute))
        created_at = (datetime.now() - minute).strftime("%Y-%m-%d %H:%M")
    elif "小时" in created_at:
        hour = created_at[:created_at.find(u"小时")]
        hour = timedelta(hours=int(hour))
        created_at = (datetime.now() - hour).strftime("%Y-%m-%d %H:%M")
    elif "今天" in created_at:
        today = datetime.now().strftime('%Y-%m-%d')
        created_at = today + ' ' + created_at[2:]
    elif '年' not in created_at:
        year = datetime.now().strftime("%Y")
        month = created_at[:2]
        day = created_at[3:5]
        time = created_at[6:]
        created_at = year + '-' + month + '-' + day + ' ' + time
    else:
        year = created_at[:4]
        month = created_at[5:7]
        day = created_at[8:10]
        time = created_at[11:]
        created_at = year + '-' + month + '-' + day + ' ' + time
    return created_at


def str_to_time(text):
    """将字符串转换成时间类型"""
    result = datetime.strptime(text, '%Y-%m-%d')
    return result
