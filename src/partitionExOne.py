from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("test1")
sc = SparkContext(conf=conf)

nums = range(0, 10) # nums = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
rdd = sc.parallelize(nums).map(lambda el: (el, el)).partitionBy(2).persist()
#partitionBy(개수) => 파티션 개수를 구하는 것
print("Number of partitions: {}".format(rdd.getNumPartitions()))
#getNumPartitions() => 파티션이 몇개가 있는가?
print("Partitioner: {}".format(rdd.partitioner))
#rdd.partitioner => 어떤 파티션 방법을 사용했는지?
print("Partitions structure: {}".format(rdd.glom().collect()))
#rdd.glom() => 파티션의 구성이 어떤지?