package geospatial.operation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier;
import com.vividsolutions.jts.geom.PrecisionModel;

public class UnionOperation {
	
	// parse results from CSV file and return Geometry Object for each rectangel
	static class ParsePoint implements Function<String, Geometry>, Serializable{   
		private static final Pattern pattern=Pattern.compile(",");
		public Geometry call(String line){
			String[] tok=pattern.split(line);
			double[] x=new double[2];
			double[] y=new double[2];
			try{
				for(int i=0; i<2; i++){
					x[i]=Double.parseDouble(tok[2+2*i]);
					y[i]=Double.parseDouble(tok[3+2*i]);
				}
				// make sure that x[1] is larger than x[0]
				double temp = 0;
				if(x[0]>x[1]){
					temp = x[0];
					x[0] = x[1];
					x[1] = temp;
				}
				if(y[0]>y[1]){
					temp = y[0];
					y[0] = y[1];
					y[1] = temp;
				}
				double weight = x[1]-x[0];
				double height = y[1]-y[0];
				
				Geometry rectangel = newGeometry(x[0], y[0], weight, height);
				
				return rectangel;
			}
			catch(ArrayIndexOutOfBoundsException e)
			{
				 try {
			            //Whatever the file path is.
			            File statText = new File("/home/jason/Downloads/vivian.txt");
			            FileOutputStream is = new FileOutputStream(statText);
			            OutputStreamWriter osw = new OutputStreamWriter(is);    
			            Writer w = new BufferedWriter(osw);
			            w.write(line);
			            w.close();
			        } catch (IOException ee) {
			            System.err.println("Problem writing to the file statsTest.txt");
			        }
				System.out.println(line);
			}
			return null;
		}
	}
	
	// create a Geometry Object for each rectangel
	private static Geometry newGeometry(double x, double y, double w,double h){
		  final GeometryFactory geometryFactory=new GeometryFactory();
		  final LinearRing linearRing=geometryFactory.createLinearRing(new Coordinate[]{new Coordinate(x,y),new Coordinate(x + w,y),new Coordinate(x + w,y + h),new Coordinate(x,y + h),new Coordinate(x,y)});
		  return (Geometry)geometryFactory.createPolygon(linearRing,null);
	}
	
	
	// union all input rectangels and return a united polygon
	public static class ComputeUnion implements FlatMapFunction<Iterator<Geometry>, Geometry>, Serializable {
	    public Iterable<Geometry> call(Iterator<Geometry> rectangel) throws Exception {
	    	ArrayList<Geometry> L = new ArrayList<Geometry>();
	    	ArrayList<Geometry> UnionResult = new ArrayList<Geometry>();
	    	while(rectangel.hasNext()){
	    		L.add(rectangel.next());
	    	}
	    	int size = L.size();
	    	Geometry[] geometries = (Geometry[])L.toArray(new Geometry[size]);
	    	//PrecisionModel floating_single = new PrecisionModel(com.vividsolutions.jts.geom.PrecisionModel.FLOATING_SINGLE);
	    	PrecisionModel floating_single = new PrecisionModel(1000000000);
	    	GeometryFactory factory = new GeometryFactory(floating_single);
	    	GeometryCollection geometryCollection =
			         (GeometryCollection) factory.createGeometryCollection(geometries);
	    	Geometry union = geometryCollection.buffer(0);
	    	UnionResult.add(union);
	    	return UnionResult;
	    }
	
	
	public static void main(String argv[]) throws IOException {
		SparkConf sparkConf = new SparkConf().setMaster("spark://192.168.146.146:7077").setAppName("Union");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		int default_slices=100;
		jsc.addJar("/home/jason/workspace/operation/target/uber-operation-0.0.1-SNAPSHOT.jar");
		JavaRDD<String> file = jsc.textFile("hdfs://master:54310//file//arealm.csv", default_slices);
	    
	    JavaRDD<Geometry> rectangel = file.map(new ParsePoint());
		JavaRDD<Geometry> res1 = rectangel.mapPartitions(new ComputeUnion());
		List<Geometry> res2 = res1.collect();
		
		int size = res2.size();
		System.out.println("number of polygon");
		System.out.println(size);
    	//PrecisionModel floating_single = new PrecisionModel(com.vividsolutions.jts.geom.PrecisionModel.FLOATING_SINGLE);
		PrecisionModel floating_single = new PrecisionModel(1000000000);
    	GeometryFactory factory = new GeometryFactory(floating_single);
    	GeometryCollection geometryCollection =
		         (GeometryCollection) factory.createGeometryCollection((Geometry[])res2.toArray(new Geometry[size]));
    	res2.clear();
    	Geometry union = geometryCollection.buffer(0);
    	List<Coordinate> coordinates = Arrays.asList(union.getCoordinates());
    	JavaRDD<String> res3 = jsc.parallelize(coordinates).distinct().map(new Function<Coordinate, String>(){
    		public String call(Coordinate p) throws Exception{
    			String s=p.toString();
    			return s;
    		}
    	});
    	// Save as Text File to HDFS
		Configuration hadoopConf=new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs=org.apache.hadoop.fs.FileSystem.get(URI.create("hdfs://master:54310"), hadoopConf);
		String output="hdfs://master:54310//file//Union";
		try{
			hdfs.delete(new org.apache.hadoop.fs.Path(output), true);
		}
		catch(IOException e){
			throw e;
		}
		res3.repartition(1).saveAsTextFile("hdfs://master:54310//file//Union");
		// Print results
		List<String> result=res3.collect();
		for (int i=0; i<result.size(); i++){
			System.out.println(result.get(i));
		}   		
	    jsc.stop();
	    System.out.println("end");
	}
}
}

