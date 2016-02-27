package geospatial.operation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;


public final class localFarthestPair {
	static class DataPoint implements Comparable<DataPoint>, Serializable {
	    /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		DataPoint(double x, double y) {
	      this.x = x;
	      this.y = y;
	    }
	    double x;
	    double y;
	    
	    public int compareTo(DataPoint p) {
	    	int res1 = Double.compare(this.x, p.x); 
			if(res1 == 0)
				return Double.compare(this.y, p.y);
			else
				return res1;
		}
	}
	
	static class ParsePoint implements PairFunction<String, DataPoint, Integer>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private static final Pattern SPACE = Pattern.compile(",");
	    public Tuple2<DataPoint,Integer> call(String line) {
	      String[] tok = SPACE.split(line);
	      Double x = Double.parseDouble(tok[2]);
	      Double y = Double.parseDouble(tok[3]);
	      return new Tuple2<DataPoint, Integer>(new DataPoint(x, y), 1);
	    }
	}
	
	static class AddDummy implements PairFunction<DataPoint, DataPoint, Integer>, Serializable {
		public Tuple2<DataPoint,Integer> call(DataPoint p) {
			return new Tuple2<DataPoint, Integer>(p,1);
		}
	}
	
	public static class TupleComparator implements Comparator<DataPoint>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public int compare(DataPoint p1, DataPoint p2) {
			int res1 = Double.compare(p1.x, p2.x); 
			if(res1 == 0)
				return Double.compare(p1.y, p2.y);
			else
				return res1;
		}
	}
	
	
	public static double cross(DataPoint O, DataPoint A, DataPoint B) {
		return ((A.x - O.x) * (B.y - O.y) - (A.y - O.y) * (B.x - O.x));
	}
	
	
	
	public static ArrayList<DataPoint> finalMonotone(ArrayList<DataPoint> myPoints) {
		ArrayList<DataPoint> L = new ArrayList<DataPoint>();
		if (myPoints.size() > 1) {
			int n = myPoints.size(), k = 0;
			DataPoint[] H = new DataPoint[2 * n];
 
			Collections.sort(myPoints);
			// Build lower hull
			for (int i = 0; i < n; ++i) {
				while (k >= 2 && cross(H[k - 2], H[k - 1], myPoints.get(i)) <= 0)
					k--;
				H[k++] = myPoints.get(i);
			}

			// Build upper hull
			for (int i = n - 2, t = k + 1; i >= 0; i--) {
				while (k >= t && cross(H[k - 2], H[k - 1], myPoints.get(i)) <= 0)
					k--;
				H[k++] = myPoints.get(i);
			}
			/*if (k > 1) {
				H = Arrays.copyOfRange(H, 0, k - 1); // remove non-hull vertices after k; remove k - 1 which is a duplicate
			}
			return H;*/
			for(int i = 0; i < k; i++)
			{
				L.add(H[i]);
			}
			return L;
		} else if (myPoints.size() <= 1) {
			return myPoints;
		} else{
			return null;
		}
	}
	
	public static class NewMonoTone implements FlatMapFunction<Iterator<Tuple2<DataPoint,Integer>>, DataPoint>, Serializable {
	    public Iterable<DataPoint> call(Iterator<Tuple2<DataPoint,Integer>> Points) throws Exception {
	    	ArrayList<DataPoint> myPoints = new ArrayList<DataPoint>();
	    	ArrayList<DataPoint> L = new ArrayList<DataPoint>();
	    	
	    	while(Points.hasNext()) {
	    		Tuple2<DataPoint,Integer> point = Points.next();
	    		myPoints.add(point._1);
	    	}
	    	if (myPoints.size() > 1) {
				int n = myPoints.size(), k = 0;
				DataPoint[] H = new DataPoint[2 * n];
	 
				// Build lower hull
				for (int i = 0; i < n; ++i) {
					while (k >= 2 && cross(H[k - 2], H[k - 1], myPoints.get(i)) <= 0)
						k--;
					H[k++] = myPoints.get(i);
				}

				// Build upper hull
				for (int i = n - 2, t = k + 1; i >= 0; i--) {
					while (k >= t && cross(H[k - 2], H[k - 1], myPoints.get(i)) <= 0)
						k--;
					H[k++] = myPoints.get(i);
				}
				/*if (k > 1) {
					H = Arrays.copyOfRange(H, 0, k - 1); // remove non-hull vertices after k; remove k - 1 which is a duplicate
				}
				return H;*/
				for(int i = 0; i < k; i++)
				{
					L.add(H[i]);
				}
				return L;
			} else if (myPoints.size() <= 1) {
				return myPoints;
			} else{
				return null;
			}
	    }
	}
	
	public static class Distance implements PairFunction<Tuple2<DataPoint,DataPoint>, Double, Tuple2<DataPoint,DataPoint>>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Tuple2<Double,Tuple2<DataPoint,DataPoint>> call(Tuple2<DataPoint,DataPoint> pair) {
			double dis = Math.pow(pair._1.x - pair._2.x, 2) + Math.pow(pair._1.y - pair._2.y, 2);
			return new Tuple2<Double, Tuple2<DataPoint,DataPoint>>(dis, pair);
		}
	}
	
	public static class compDis implements Comparator<Tuple2<Double,Tuple2<DataPoint,DataPoint>>>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public int compare(Tuple2<Double,Tuple2<DataPoint,DataPoint>> p1, Tuple2<Double,Tuple2<DataPoint,DataPoint>> p2) {
			return Double.compare(p1._1, p2._1); 
		}
	}
	
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setMaster("spark://192.168.146.146:7077").setAppName("FarthestPoint");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
	    jsc.addJar("/home/jason/workspace/operation/target/uber-operation-0.0.1-SNAPSHOT.jar");
	    int default_slices=100;
	    JavaRDD<String> file = jsc.textFile("hdfs://master:54310//file//arealm.csv", default_slices);	    
	    JavaPairRDD<DataPoint,Integer> points = file.mapToPair(new ParsePoint());
	    JavaPairRDD<DataPoint,Integer> sortedPointsAsc = points.sortByKey(new TupleComparator(), true, default_slices);

	    List<DataPoint> res = sortedPointsAsc.mapPartitions(new NewMonoTone()).distinct().collect();
	    ArrayList<DataPoint> res1 = finalMonotone((ArrayList<DataPoint>)res);
	    JavaRDD<DataPoint> res2 = jsc.parallelize(res1);
	    List<Tuple2<Double,Tuple2<DataPoint,DataPoint>>> res3 = res2.cartesian(res2).mapToPair(new Distance()).top(1, new compDis());
	    List<String> temp=new ArrayList<String>(2);
	    temp.add(res3.get(0)._2._1.x + "," + res3.get(0)._2._1.y);
	    temp.add(res3.get(0)._2._2.x + "," + res3.get(0)._2._2.y);
	    JavaRDD<String> out=jsc.parallelize(temp);
		// Save as Text File to HDFS
		Configuration hadoopConf=new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs=org.apache.hadoop.fs.FileSystem.get(URI.create("hdfs://master:54310"), hadoopConf);
		String output="hdfs://master:54310//file//FarthestPair";
		try{
			hdfs.delete(new org.apache.hadoop.fs.Path(output), true);
		}
		catch(IOException e){
			throw e;
		}
		out.repartition(1).saveAsTextFile("hdfs://master:54310//file//FarthestPair");
		// Print Results
		List<String> result=out.collect();
		for(int i=0; i<result.size(); i++){
			System.out.println(result.get(i));
		}
		jsc.stop();
	}
}