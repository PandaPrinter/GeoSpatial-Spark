package geospatial.operation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

public class ClosestPair {
	  public static class Point
	  {
	    public double x;
	    public double y;

	    public Point(double x, double y)
	    {
	      this.x = x;
	      this.y = y;
	    }
	  }
	static class ParsePoint implements Function<String, Tuple2<Double,Double>>,  Serializable{
		private static final Pattern SPACE = Pattern.compile(",");
	
		public Tuple2<Double,Double> call(String line) {
			String[] tok = SPACE.split(line);
	    	Double x = Double.parseDouble(tok[2]);
	    	Double y = Double.parseDouble(tok[3]);
	    	return new Tuple2<Double,Double>(x,y);
	    }
	}
	
	private static class sortX implements Function<Tuple2<Double, Double>, Double>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Double call(Tuple2<Double,Double> a) {
			return a._1;
		}
	}
	
	
	  public static double distance(Tuple2<Double,Double> p1, Tuple2<Double,Double> p2)
	  {
	    double xdist = p2._1 - p1._1;
	    double ydist = p2._2 - p1._2;
	    return Math.pow(xdist, 2) + Math.pow(ydist, 2);
	  }
	
	
	  public static void sortByX(List<? extends Tuple2<Double,Double>> points)
	  {
	    Collections.sort(points, new Comparator<Tuple2<Double,Double>>() {
	        public int compare(Tuple2<Double,Double> point1, Tuple2<Double,Double> point2)
	        {
	          if (point1._1 < point2._1)
	            return -1;
	          if (point1._1 > point2._1)
	            return 1;
	          return 0;
	        }
	      }
	    );
	  }
	  

	  public static void sortByY(List<? extends Tuple2<Double,Double>> points)
	  {
	    Collections.sort(points, new Comparator<Tuple2<Double,Double>>() {
	        public int compare(Tuple2<Double,Double> point1, Tuple2<Double,Double> point2)
	        {
	          if (point1._2 < point2._2)
	            return -1;
	          if (point1._2 > point2._2)
	            return 1;
	          return 0;
	        }
	      }
	    );
	  }
	  
	  public static Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> bruteForce(List<? extends Tuple2<Double,Double>> points)
	  {
	    int numPoints = points.size();
	    if (numPoints < 2)
	      return null;
	    Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> pair = new Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double>(points.get(0), points.get(1),distance(points.get(0),points.get(1)));
	    if (numPoints > 2)
	    {
	      for (int i = 0; i < numPoints - 1; i++)
	      {
	    	Tuple2<Double,Double> point1 = points.get(i);
	        for (int j = i + 1; j < numPoints; j++)
	        {
	          Tuple2<Double,Double> point2 = points.get(j);
	          double distance = distance(point1, point2);
	          if (distance < pair._3())
	            pair = new Tuple3(point1, point2, distance);
	        }
	      }
	    }
	    return pair;
	  }
	  
	  public static Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> divideAndConquer(List<? extends Tuple2<Double,Double>> points)
	  {
	    List<Tuple2<Double,Double>> pointsSortedByY = new ArrayList<Tuple2<Double,Double>>(points);
	    sortByY(pointsSortedByY);
	    return divideAndConquer(points, pointsSortedByY);
	  }
	  
	  private static Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> divideAndConquer(List<? extends Tuple2<Double,Double>> pointsSortedByX, List<? extends Tuple2<Double,Double>> pointsSortedByY)
	  {
	    int numPoints = pointsSortedByX.size();
	    if (numPoints <= 3)
	      return bruteForce(pointsSortedByX);

	    int dividingIndex = numPoints >>> 1;
	    List<? extends Tuple2<Double, Double>> leftOfCenter = pointsSortedByX.subList(0, dividingIndex);
	    List<? extends Tuple2<Double, Double>> rightOfCenter = pointsSortedByX.subList(dividingIndex, numPoints);
	    List<Tuple2<Double,Double>> tempList = new ArrayList<Tuple2<Double,Double>>(leftOfCenter);
	    sortByY(tempList);
	    Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> closestPair = divideAndConquer(leftOfCenter, tempList);

	    tempList.clear();
	    tempList.addAll(rightOfCenter);
	    sortByY(tempList);
	    Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> closestPairRight = divideAndConquer(rightOfCenter, tempList);

	    if (closestPairRight._3() < closestPair._3())
	      closestPair = closestPairRight;

	    tempList.clear();
	    double shortestDistance =closestPair._3();
	    double centerX = rightOfCenter.get(0)._1;
	    for (Tuple2<Double,Double> point : pointsSortedByY)
	      if (Math.abs(centerX - point._1) < shortestDistance)
	        tempList.add(point);

	    for (int i = 0; i < tempList.size() - 1; i++)
	    {
	      Tuple2<Double,Double> point1 = tempList.get(i);
	      for (int j = i + 1; j < tempList.size(); j++)
	      {
	    	Tuple2<Double,Double> point2 = tempList.get(j);
	        if ((point2._2 - point1._2) >= shortestDistance)
	          break;
	        double distance = distance(point1, point2);
	        if (distance < closestPair._3())
	        {
	          closestPair = new Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double>(point1, point2, distance);
	          shortestDistance = distance;
	        }
	      }
	    }
	    return closestPair;
	  }
	  
	private static class DcAlgorithm implements Function2<Integer, Iterator<Tuple2<Double,Double>>, Iterator<Tuple4<Integer,Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>,Double,Tuple2<Double,Double>>>>, Serializable {
		public Iterator<Tuple4<Integer,Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>,Double,Tuple2<Double,Double>>> call(Integer a, Iterator<Tuple2<Double,Double>> p) {
			ArrayList<Tuple4<Integer,Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>,Double,Tuple2<Double,Double>>> res = new ArrayList<Tuple4<Integer,Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>,Double,Tuple2<Double,Double>>>();
			// pointList contains points in each partition
			List<Tuple2<Double,Double>> pointList = new ArrayList<Tuple2<Double,Double>>();
			double minX, maxX;
			int count = 0;
			while(p.hasNext())
			{
				pointList.add(p.next());
				count = count + 1;
			}
			minX = pointList.get(0)._1;
			maxX = pointList.get(count-1)._1;
			
			double minDis = Double.MAX_VALUE;
			Tuple2<Double,Double> p1 = pointList.get(0), p2 = pointList.get(1);

			Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> minDistanceWithPoints = divideAndConquer(pointList);
			
			res.add(new Tuple4<Integer,Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>,Double,Tuple2<Double,Double>>(a, new Tuple2(minDistanceWithPoints._1(),minDistanceWithPoints._2()), minDistanceWithPoints._3(), new Tuple2(minX, maxX)));
			return res.iterator();
		}
	}
	
	private static Tuple4<Integer,Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>,Double,Tuple2<Double,Double>> DcAlgorithm(Iterator<Tuple2<Double, Double>> a){
		ArrayList<Tuple4<Integer,Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>,Double,Tuple2<Double,Double>>> res = new ArrayList<Tuple4<Integer,Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>,Double,Tuple2<Double,Double>>>();
		// pointList contains points in each partition
		List<Tuple2<Double,Double>> pointList = new ArrayList<Tuple2<Double,Double>>();
		double minX, maxX;
		int count = 0;
		while(a.hasNext())
		{
			pointList.add(a.next());
			count = count + 1;
		}
		minX = pointList.get(0)._1;
		maxX = pointList.get(count-1)._1;
		
		double minDis = Double.MAX_VALUE;
		Tuple2<Double,Double> p1 = pointList.get(0), p2 = pointList.get(1);

		Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> minDistanceWithPoints = divideAndConquer(pointList);
		
		return new Tuple4<Integer,Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>,Double,Tuple2<Double,Double>>(0, new Tuple2(minDistanceWithPoints._1(),minDistanceWithPoints._2()), minDistanceWithPoints._3(), new Tuple2(minX, maxX));
	}
	
	private static Double GetTempShortestDis(JavaRDD<Tuple4<Integer, Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>,Double,Tuple2<Double,Double>>> b){
		List<Tuple4<Integer, Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>,Double,Tuple2<Double,Double>>> a = b.collect();
		Double answer = Double.MAX_VALUE;
		
		for(int i=0; i<a.size(); i++){
			answer = (a.get(i)._3() < answer && a.get(i)._3() > 0) ? a.get(i)._3() : answer;
		}
		return answer;
	}
	
	private static JavaRDD<Tuple2<Double, Double>> GetLeftPoints(JavaRDD<Tuple2<Double,Double>> points, final Double distance){
		return points.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Double, Double>>, Iterator<Tuple2<Double, Double>>>(){

			@Override
			public Iterator<Tuple2<Double, Double>> call(Integer v1, Iterator<Tuple2<Double, Double>> v2) throws Exception {
				// TODO Auto-generated method stub
				List<Tuple2<Double, Double>> points = new ArrayList<Tuple2<Double, Double>>();
				List<Tuple2<Double, Double>> re = new ArrayList<Tuple2<Double, Double>>();
				while(v2.hasNext()){
					points.add(v2.next());
				}
				
				if(v1 == 0){
					for(Tuple2<Double, Double> point : points){
						if((points.get(points.size()-1)._1() - point._1()) < distance){
							re.add(point);
						}
					}
				}else{
					for(Tuple2<Double, Double> point : points){
						if(((points.get(points.size()-1)._1() - point._1()) < distance)||((point._1() - points.get(0)._1())<distance)){
							re.add(point);
						}
					}
				}
				
				return re.iterator();
			}

		}, true);
	}

	
	public static void main(String[] args) {
			Integer numPartition = 100;
			SparkConf sparkConf = new SparkConf().setMaster("spark://192.168.215.136:7077").setAppName("ClosetPair");
			JavaSparkContext jsc = new JavaSparkContext(sparkConf);
			jsc.addJar("/home/danielvm/workspace/operation1/target/uber-operation1-0.0.1-SNAPSHOT.jar");
			JavaRDD<String> file = jsc.textFile("hdfs://master:54310//data//arealm.csv");
			JavaRDD<Tuple2<Double,Double>> points = file.map(new ParsePoint()).sortBy(new sortX(), true, numPartition).cache();
			points = points.distinct();
			JavaRDD<Tuple4<Integer, Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>,Double,Tuple2<Double,Double>>> res = points.mapPartitionsWithIndex(new DcAlgorithm(), true);
			
			Double tempShortestDis = GetTempShortestDis(res);
			JavaRDD<Tuple2<Double, Double>> leftPoints = GetLeftPoints(points, tempShortestDis);
			numPartition = (int) (leftPoints.count()/1000);
			
			while(numPartition > 1){
				leftPoints = leftPoints.repartition(numPartition);
				res = leftPoints.mapPartitionsWithIndex(new DcAlgorithm(), true);
				tempShortestDis = GetTempShortestDis(res);
				leftPoints = GetLeftPoints(leftPoints, tempShortestDis);
				numPartition = (int) (leftPoints.count()/1000);
				System.out.println("Twice" + numPartition);
			}
			
			Tuple4<Integer,Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>,Double,Tuple2<Double,Double>> x = DcAlgorithm(leftPoints.collect().iterator());
			System.out.println(x._2()._1()._1() + " " + x._2()._1()._2());
			System.out.println(x._2()._2()._1() + " " + x._2()._2()._2());
			System.out.println(x._3());
		}
}
