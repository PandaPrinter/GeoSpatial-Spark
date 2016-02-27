package geospatial.operation;

import geospatial.operation.InputClassForJoin.input1;
import geospatial.operation.InputClassForJoin.input2;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class HeatMap {

	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("HeatMap").setMaster(
				"spark://192.168.204.162:7077");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.addJar("/home/jason/workspace/operation/target/uber-operation-0.0.1-SNAPSHOT.jar");

		JavaRDD<input1> rdd_records1 = sc
				.textFile("hdfs://master:54310//data//arealm.csv", 100)
				.map(new Function<String, input1>() {
					public input1 call(String line) throws Exception {
						String[] fields = line.split(",");
						input1 sd1 = new input1(fields[1].trim(), fields[2]
								.trim(), fields[3].trim(), fields[4].trim(),
								fields[5].trim());
						return sd1;
					}
				}).cache();
		final JavaRDD<input1> SortedRectangle = rdd_records1.sortBy(
				new Function<input1, Double>() {
					public Double call(input1 p) throws Exception {
						return p.getx2();
					}
				}, true, 100);

		final JavaRDD<input2> rdd_records2 = sc
				.textFile("hdfs://master:54310//data//areawater.csv", 100)
				.map(new Function<String, input2>() {
					public input2 call(String line) throws Exception {
						String[] fields = line.split(",");
						input2 sd2 = new input2(fields[2].trim(), fields[3]
								.trim());
						return sd2;
					}
				}).cache();

		List<input2> list = new ArrayList<input2>();
		for (input2 rddData : rdd_records2.collect()) {
			list.add(rddData);
		}
		final Broadcast<List<input2>> list1 = sc.broadcast(list);

		JavaPairRDD<String, Integer> pairs = SortedRectangle
				.mapToPair(new PairFunction<input1, String, Integer>() {
					public Tuple2<String, Integer> call(input1 s)
							throws Exception {
						int count = 0;
						for (input2 l : list1.value()) {
							if (s.getx2() < l.getx1() && l.getx1() < s.getx1()
									&& s.gety2() < l.gety1()
									&& l.gety1() < s.gety1()) {
								count += 1;
							}
						}
						return new Tuple2<String, Integer>(s.getid(), count);
					}
				});
		// Save as Text File to HDFS

		Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(
				URI.create("hdfs://master:54310"), hadoopConf);
		String output = "hdfs://master:54310//data//HeatMap";
		try {
			hdfs.delete(new org.apache.hadoop.fs.Path(output), true);
		} catch (IOException e) {
			throw e;
		}
		pairs.repartition(1).saveAsTextFile(
				"hdfs://master:54310//data//HeatMap");
	}
}
