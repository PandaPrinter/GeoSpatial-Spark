package geospatial.operation;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.math.Ordering;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;

public final class localConvex {
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
	
	
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setMaster("spark://192.168.146.146:7077").setAppName("ConvexHull");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		int default_slices=100;
	    jsc.addJar("/home/jason/workspace/operation/target/uber-operation-0.0.1-SNAPSHOT.jar");
	    JavaRDD<String> file = jsc.textFile("hdfs://master:54310//file//arealm.csv",default_slices);
	    
	    JavaPairRDD<DataPoint,Integer> points = file.mapToPair(new ParsePoint());
	    JavaPairRDD<DataPoint,Integer> sortedPointsAsc = points.sortByKey(new TupleComparator(), true, default_slices);

	    List<DataPoint> res = sortedPointsAsc.mapPartitions(new NewMonoTone()).distinct().collect();
	    ArrayList<DataPoint> res1 = finalMonotone((ArrayList<DataPoint>)res);
	    Collections.reverse(res1);    // Clock-wise 
	    res1.remove(res1.size()-1);   // the first point and the last one are same, thus remove
	    // Change it back to RDD in order to saveAsTextFile
	    JavaRDD<String> out=jsc.parallelize(res1).map(new Function<DataPoint, String>(){
	    	public String call(DataPoint p) throws Exception{
	    		String s= p.x +",  "+ p.y;
	    		return s;
	    	}
	    });
		// Save as Text File to HDFS
		Configuration hadoopConf=new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs=org.apache.hadoop.fs.FileSystem.get(URI.create("hdfs://master:54310"), hadoopConf);
		String output="hdfs://master:54310//file//ConvexHull";
		try{
			hdfs.delete(new org.apache.hadoop.fs.Path(output), true);
		}
		catch(IOException e){
			throw e;
		}
		out.repartition(1).saveAsTextFile("hdfs://master:54310//file//ConvexHull");
		// Print results
	    List<String> result=out.collect();
    	for(int i = 0; i < result.size(); i++) {
    		System.out.println(result.get(i));
    	} 	
    	jsc.stop();
	}
}