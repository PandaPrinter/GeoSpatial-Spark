package geospatial.operation;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.vividsolutions.jts.geom.Coordinate;

import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.net.URI;
import java.util.regex.Pattern;
import java.util.*;

public class RangeQuery {
	static class PointPairs implements Serializable{
		double[] x;
		double id;
		PointPairs(double id, double[] x){
			this.x=x; this.id=id;
		}
	}
	// parse results with id and (x1,y1,x2,y2) from CSV file
	static class ParsePointPairs implements Function<String, PointPairs>, Serializable{   
		private static final Pattern pattern=Pattern.compile(",");
		public PointPairs call(String line) throws Exception{
			int N=4;
			String[] tok=pattern.split(line);
			double id=Double.parseDouble(tok[1]);
			double[] x=new double[N];
			for(int i=0; i<N; i++){
				x[i]=Double.parseDouble(tok[i+2]);
			}
			return new PointPairs(id, x);
		}
	}
	public static void main(String args[]) throws IOException{
		SparkConf sparkConf = new SparkConf().setMaster("spark://192.168.146.146:7077").setAppName("RangeQuery");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.addJar("/home/jason/workspace/operation/target/uber-operation-0.0.1-SNAPSHOT.jar");
		int default_slices=100;
		JavaRDD<String> lines = sc.textFile("hdfs://master:54310//file//arealm.csv",default_slices);
		final Broadcast<double[]> query=sc.broadcast(new double[] {-84,30,-81,33});
		//final Broadcast<double[]> query=sc.broadcast(new double[] {0,0,1,1});
		JavaRDD<PointPairs> points = lines.map(new ParsePointPairs());   // get id and (x1,y1,x2,y2)
		JavaRDD<PointPairs> result=points.filter(new Function<PointPairs, Boolean>(){
			public Boolean call(PointPairs p) throws Exception{
				if (Math.min(p.x[0], p.x[2])>=Math.min(query.value()[0], query.value()[2]) && Math.max(p.x[0], p.x[2])<=Math.max(query.value()[0], query.value()[2])
						&& Math.min(p.x[1], p.x[3])>=Math.min(query.value()[1], query.value()[3]) && Math.max(p.x[1], p.x[3])<=Math.max(query.value()[1], query.value()[3]))
					return true;
				else 
					return false;
			}
		});
		JavaRDD<String> str=result.map(new Function<PointPairs, String>(){
			public String call(PointPairs p) throws Exception{
				String s="ID: "+p.id+"  Coordinates:  "+ p.x[0]+", "+p.x[1]+
						";   "+p.x[2]+",  "+p.x[3];
				return s;
			}
		});
		// Save as Text File to HDFS
		Configuration hadoopConf=new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs=org.apache.hadoop.fs.FileSystem.get(URI.create("hdfs://master:54310"), hadoopConf);
		String output="hdfs://master:54310//file//RangeQuery";
		try{
			hdfs.delete(new org.apache.hadoop.fs.Path(output), true);
		}
		catch(IOException e){
			throw e;
		}
		str.repartition(1).saveAsTextFile("hdfs://master:54310//file//RangeQuery");
		// Print results
		List<String> res=str.collect();
		for (int i=0; i<res.size(); i++){
			System.out.println(res.get(i));
		}
		sc.stop();
	}
}
