package Spark_Mahout.spark_mahout;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import scala.Tuple2;
import scala.reflect.ClassTag$;

/**
 * Utilities class
 * @author yoelluginbuhl
 *
 */
public class Utils {
	
	/**
	 * Prints nicely the timings from a start and end time and a given string.
	 * Format: "test [ns] [ms] [sec] [min]"
	 * @param t_start
	 * @param t_end
	 * @param text
	 */
	public static void printTiming(long t_start, long t_end, String text) {
		long ns = t_end - t_start;
		double ms = ns / 1000000;
		double s = ms / 1000;
		double m = s / 60;
		System.out.println(text+" "+ns+"[ns] \t"+ms+"[ms] \t"+s+"[sec] \t"+m+"[min]");
	}
	
	/**
	 * Helper method to print a list of SparkMahoutRecommendation object
	 * @param recommendations	List<SparkMahoutRecommendation>
	 */
	static void printRatingRecommendations(List<SparkMahoutRecommendation> recommendations) {
		System.out.println("Recommended Items:");
		for (SparkMahoutRecommendation recom : recommendations) {
			System.out.println("\t"+recom);
		}
	}

	/**
	 * Helper method to print a Datase<Row> object
	 * @param ds		Dataset<Row>
	 */
	static void printRecommendations(Dataset<Row> ds) {
		System.out.println("Print recommendation..............");

		long s0 = System.nanoTime();
		String str = ds.toString();
		long s1 = System.nanoTime();

		System.out.println(str);
		Utils.printTiming(s0, s1, "ds.toString time: ");

		long j0 = System.nanoTime();
		Dataset<String> jsons = ds.toJSON();
		long j1 = System.nanoTime();
		Utils.printTiming(j0, j1, "ds.toJSON time: ");
		long a0 = System.nanoTime();
		jsons.showString(1, 20);
		long a1 = System.nanoTime();
		Utils.printTiming(a0, a1, "jsons.showString() time: ");

	}

	/**
	 * Parse a string into a boolean value. True will be returned for "yes", "1" or "true"
	 * Flase will be returned in any other case.
	 * @param val String
	 * @return boolean
	 */
	public static boolean parseBooleanString(String val) {
		if (val.equalsIgnoreCase("true") || val.equalsIgnoreCase("yes") || val.equalsIgnoreCase("1"))
			return true;
		else
			return false;
	}
	
	/**
	 * Retrieves the path of the model in the system.
	 * @return String modelPath
	 */
	public static String getModelPath() {
		String modelPath = "model";
		File file = new File(modelPath);
		if(!file.exists()) {
			file.mkdir();
		}
		
		return modelPath;
	}
	
	/**
	 * Retrieves the path of the application.
	 * @return String root path of the application.
	 */
	public static String getGetApplicationPath() {
		URL url = Utils.class.getProtectionDomain().getCodeSource().getLocation();
		String parentPath = "";
		try {
			String jarPath = URLDecoder.decode(url.getFile(), "UTF-8");
			parentPath = new File(jarPath).getParentFile().getPath();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		return parentPath;
	}

	/**
	 * Utility method in order to get around scala-java compatibility
	 * from: https://github.com/OryxProject/oryx/blob/f75dce25e45691d972c8d1713cc1080db7f27408/app/oryx-app-mllib/src/main/java/com/cloudera/oryx/app/batch/mllib/als/ALSUpdate.java
	 * @param rdd
	 * @return
	 */
	static <K,V> JavaPairRDD<K,V> fromRDD(RDD<Tuple2<K,V>> rdd) {
		return JavaPairRDD.fromRDD(rdd,
				ClassTag$.MODULE$.apply(Object.class),
				ClassTag$.MODULE$.apply(Object.class));
	}
	/**
	 * Utility method in order to get around scala-java compatibility
	 * from: https://github.com/OryxProject/oryx/blob/f75dce25e45691d972c8d1713cc1080db7f27408/app/oryx-app-mllib/src/main/java/com/cloudera/oryx/app/batch/mllib/als/ALSUpdate.java
	 * @param in
	 * @return
	 */
	static <A,B> JavaPairRDD<Integer,B> massageToIntKey(RDD<Tuple2<A,B>> in) {
		// More horrible hacks to get around Scala-Java unfriendliness
		@SuppressWarnings("unchecked")
		JavaPairRDD<Integer,B> javaRDD = fromRDD((RDD<Tuple2<Integer,B>>) (RDD<?>) in);
		return javaRDD;
	}
	/**
	 * Utility method in order to get around scala-java compatibility
	 * from: https://github.com/OryxProject/oryx/blob/f75dce25e45691d972c8d1713cc1080db7f27408/app/oryx-app-mllib/src/main/java/com/cloudera/oryx/app/batch/mllib/als/ALSUpdate.java
	 */
	static Function<double[],float[]> doubleArrayToFloats = d -> {
		float[] f = new float[d.length];
		for (int i = 0; i < f.length; i++) {
			f[i] = (float) d[i];
		}
		return f;
	};

	/**
	 * Converts a list<float[]> into a float[][]
	 * 
	 * @param list	List<float[]>
	 * @return	float[][]
	 */
	static float[][] toDoubleFloatArray(List<float[]> list) {
		int nbRows = list.size();
		int nbColumns = list.get(0).length;
		float[][] ddf = new float[nbRows][nbColumns];
		for (int i = 0; i < ddf.length; i++) {
			float[] tmp = list.get(i);
			for (int j = 0; j < ddf[i].length; j++) {
				ddf[i][j] = tmp[j];
			}
		}
		return ddf;
	}

	/**
	 * Converts a List<double[]> into a double[][]
	 * @param list	List<double[]>
	 * @return double[][]
	 */
	static double[][] toDoubleDoubleArray(List<double[]> list) {
		int nbRows = list.size();
		int nbColumns = list.get(0).length;
		double[][] ddf = new double[nbRows][nbColumns];
		for (int i = 0; i < ddf.length; i++) {
			double[] tmp = list.get(i);
			for (int j = 0; j < ddf[i].length; j++) {
				ddf[i][j] = tmp[j];
			}
		}
		return ddf;
	}

	/**
	 * Retrieve the top n columns from the matrix
	 * 
	 * @param a INDArray the matrix to look in
	 * @param n	int how many columns should it retrieve 
	 * @return INDArray a new matrix containing the top n columns.
	 */
	static INDArray getTopN(INDArray a, int n) {
		INDArray topN = Nd4j.zeros(n);
		INDArray sorted_cols = Nd4j.sortColumns(a, 0, false);

		for (int i = 0; i < topN.getRow(0).columns(); i++) {
			topN.put(i, sorted_cols.getColumn(i));
		}

		return topN;
	}
}
