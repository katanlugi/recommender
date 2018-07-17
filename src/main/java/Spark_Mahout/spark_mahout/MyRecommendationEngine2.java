package Spark_Mahout.spark_mahout;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
//import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

/**
 * Test of a recommender running on the org.apache.spark.ml library
 * Currently finished and thus not working properly.
 * 
 * @author yoelluginbuhl
 *
 */
public class MyRecommendationEngine2 {
	protected static ALSModel model;


	public static void main(String[] args) {
		args = new String[] {
				"data/big/ratings.csv",	// path of file containing the ratings
				"5", 					// numIterations
				"false"					// should evaluate trained model
		};

//		MyRecommendationEngine2 engine = new MyRecommendationEngine2();

//		java.lang.Class<?>[] classes = new java.lang.Class[] {MyRecommendationEngine2.class};
		SparkSession spark = SparkSession
				.builder()
				.appName("Spark Recommender (Mahout)")
				.master("local[*]")
				.config("spark.ui.enabled", "false")
				.config("spark.ui.showConsoleProgress", "false")
				.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.config("spark.executor.memory", "4g")
				.getOrCreate();

		StructType schemaRatings = new StructType()
				.add("user_id", "int")
				.add("movie_id", "int")
				.add("rating", "double");

//		Dataset<Row> ratings = spark.read()
//				.option("header", false)
//				.schema(schemaRatings)
//				.csv(args[0])
//				.persist(StorageLevel.MEMORY_ONLY());
		Dataset<Row> ratings = spark.read()
				.option("header", false)
				.schema(schemaRatings)
				.csv(args[0])
				.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
		
		
		ratings.show();
		
		Dataset<Row>[] splits = ratings.randomSplit(new double[] {0.8, 0.2});
		Dataset<Row> training = splits[0];
		Dataset<Row> testing = splits[1];
		
		training.cache();
		testing.cache();

		// Build the recommendation model using ALS on the training data
		ALS als = new ALS()
				.setMaxIter(Integer.parseInt(args[1]))
				.setRegParam(0.01)
				.setUserCol("user_id")
				.setItemCol("movie_id")
				.setRatingCol("rating");
		model = als.fit(training);
		// Evaluate the model by computing the RMSE on the test data
		// Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
		model.setColdStartStrategy("drop");
		Dataset<Row> predictions = model.transform(testing);

		RegressionEvaluator evaluator = new RegressionEvaluator()
				.setMetricName("rmse")
				.setLabelCol("rating")
				.setPredictionCol("prediction");
		Double rmse = evaluator.evaluate(predictions);
		System.out.println("Root-mean-square error = "+rmse);

		// Generate top 5 movie recommendations for the first 3 users
//		Dataset<Row> users = ratings.select(als.getUserCol()).distinct().limit(3);

		long a0 = System.nanoTime();
		/** 
		 * ============================================================================================
		 * recommendForUserSubset will only be availale in Spark 2.3.0 which is currently not released.
		 * Using RDDs recommend for one user needs about 80ms while using the new Dataset
		 * recommendForAll needs about 300ms. It is longer but good enough until
		 * 2.3.0 is publicly released.
		 * ============================================================================================
		 */
		Dataset<Row> top5Req = model.recommendForAllUsers(5);
		// ============================================================================================
		long a1 = System.nanoTime();

		printTiming(a0, a1, "Recommend all: ");
		long f0 = System.nanoTime();
//		Dataset<Row> first3User = top5Req.where("user_id < 4").limit(3);
		Dataset<Row> first3User = top5Req.where("user_id = 1").limit(3);
		long f1 = System.nanoTime();
		printTiming(f0, f1, "filter < 4: ");
		first3User.show(false);
		long end = System.nanoTime();
		printTiming(a0, end, "Recommendation and display of recommendations timing: ");
		spark.stop();
	}

	public static void printTiming(long t_start, long t_end, String text) {
		long ns = t_end - t_start;
		double ms = ns / 1000000;
		double s = ms / 1000;
		double m = s / 60;
		System.out.println(text+" "+ns+"[ns] \t"+ms+"[ms] \t"+s+"[sec] \t"+m+"[min]");
	}

	public MyRecommendationEngine2() {
		// Turn off unnecessary logging
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN);
	}

}
