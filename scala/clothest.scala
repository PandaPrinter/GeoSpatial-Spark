val data = sc.textFile("/home/root123/Points.txt").map(line => line.split(", "))
val points = data.map(a => (a(0).toDouble, a(1).toDouble))
data.persist()
val spoints = points.sortByKey()
val rpoints = points.sortByKey(false)
object MyFunc1 {
  def leftTurn(p1: (Double, Double), p2: (Double, Double), p3: (Double, Double)): Boolean = {
    val slope = (p2._1 - p1._1) * (p3._2 - p1._2) - (p2._2 - p1._2) * (p3._1 - p1._1)
    val collinear = Math.abs(slope) <= 1e-9
    val leftTurn = slope < 0
    collinear || leftTurn 
	}
	}
object MyFunc3 {
    def listbuf(p1: (Double, Double)): ListBuffer[(Double, Double)] = {
	val newBuff = new ListBuffer[(Double, Double)]
	newBuff.prepend(p1)
	return newBuff
	}}
	
object MyFunc2 {
  def halfHull(p1: ListBuffer[(Double, Double)], p2: ListBuffer[(Double, Double)]): ListBuffer[(Double, Double)] = {
   while (p1.size >= 2 && MyFunc1.leftTurn(p2(0), p1(0), p1(1))) {
      p1.remove(0)
    }
    p1.prepend(p2(0))
	return p1
	}
}

val abc = spoints.map(MyFunc3.listbuf)
val upperHull = abc.reduce(MyFunc2.halfHull)
val defi = rpoints.map(MyFunc3.listbuf)
val lowerHull = defi.reduce(MyFunc2.halfHull)

val convexHull = upperHull ++: lowerHull
val output = sc.parallelize(convexHull.distinct)

val ddata = output.cartesian(output)
val cardata = ddata.filter(a => (a._2._1 != a._1._1) && (a._2._2 != a._1._2))
val dist = cardata.map(a => (a, Math.pow((a._2._1 - a._1._1),2) + Math.pow((a._2._2 - a._1._2), 2)))

val res = dist.reduce((a,b) => (if(a._2<b._2) a else b))

val closestPoints = (res._1._1, res._1._2)
