from pyspark import SparkContext, SparkConf

def country_partitioner(country):
    return hash(country)
#매개변수를 hash한다. 파티션 방법을 직접 지정했다

conf = SparkConf().setMaster("local").setAppName("test1")
sc = SparkContext(conf=conf)

transactions = [
    {'name': 'Bob', 'amount': 100, 'country': 'United Kingdom'},
    {'name': 'James', 'amount': 15, 'country': 'United Kingdom'},
    {'name': 'Marek', 'amount': 51, 'country': 'Poland'},
    {'name': 'Johannes', 'amount': 200, 'country': 'Germany'},
    {'name': 'Paul', 'amount': 75, 'country': 'Poland'},
]
#한번에 데이터를 저장하고 처리하는 방법
#list, dictionary, tuple이 파이썬에서 제공된다.

#list = 자유로운 배열로 생각한다. []로 묶어져있다.
#ex)list = [], list = [1,2], list = [1,"ab",2]

#dictionary = 사전이다. key와 Value를 같이 묶어 저장한다. {}로 묶어져있다.
#ex) dictionary = {'name':'Bob','sum':100}

rdd = sc.parallelize(transactions).map(lambda el: (el['country'], el)).partitionBy(2, country_partitioner)
#partitionByKey(파티션개수,커스텀함수)

print("Number of partitions: {}".format(rdd.getNumPartitions()))
print("Partitioner: {}".format(rdd.partitioner))
print("Partitions structure: {}".format(rdd.glom().collect()))