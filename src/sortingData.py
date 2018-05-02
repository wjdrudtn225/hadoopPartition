Custom sort order in Python sorting integers as if strings
rdd.sortByKey(ascending=True, numPartitions=None, keyfunc = lambda x: str(x))
Custom sort order in Scala sorting integers as if strings
val input: RDD[(Int, Venue)] = ...
implicit val sortIntegersByString = new Ordering[Int] {
	override def compare(a: Int, b: Int) = a.toString.compare(b.toString)
}
rdd.sortByKey()

Custom sort order in Java sorting integers as if strings
class IntegerComparator implements Comparator<Integer> {
	public int compare(Integer a, Integer b) {
		return String.valueOf(a).compareTo(String.valueOf(b))
	}
}
rdd.sortByKey(comp)