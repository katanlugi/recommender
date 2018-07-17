package Spark_Mahout.spark_mahout;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

public class RecommendationEngine {
	public boolean debug = true;
	private static String modelPath = "data/model";
	private static StructType schemaRatings = new StructType()
			.add("user_id", "int")
			.add("movie_id", "int")
			.add("rating", "double");

	private static Dataset<Row> allRecommendations;
	private String ratingsFilePath;
	private SparkSession spark;
	private ALSModel model;

	public RecommendationEngine(String ratingsFilePath) {
		this.ratingsFilePath = ratingsFilePath;
		configureLoggingOptions();
		initializeSparkSession();
		initializeModel();
	}

//	public Dataset<Row> recomendFirst3(int nbItems) {
//		if (model == null) {
//			loadOrTrainModel();
//		}
//		if (shouldExecuteRecommendations()) {
//			if (debug) System.out.println("executing model.recommendForAllUsers()...");
//			long t0 = System.nanoTime();
//			allRecommendations = model.recommendForAllUsers(nbItems);
//			long t1 = System.nanoTime();
//			Utils.printTiming(t0, t1, "recommendForAllUsers() timing: "); 
//			lastFullRecomendationTime = System.nanoTime();
//		}
//		long f0 = System.nanoTime();
//		Dataset<Row> first3User = allRecommendations.where("user_id < 4").limit(3);
//		long f1 = System.nanoTime();
//		Utils.printTiming(f0, f1, "Filter time: ");
//		//		if (debug) first3User.show(false);
//		return first3User;
//	}
	
	public void stop() {
		spark.stop();
		spark.close();
	}
	
	public Dataset<Row> recommend(int userId, int nbItems) {
//		if (shouldExecuteRecommendations()) {
			if (debug) System.out.println("executing model.recommendForAllUsers()...");
			long t0 = System.nanoTime();
			allRecommendations = model.recommendForAllUsers(nbItems);
//			System.out.println("nb recommendations: "+allRecommendations.count());
			long t1 = System.nanoTime();
			Utils.printTiming(t0, t1, "recommendForAllUsers() timing: "); 
//		}
		long f0 = System.nanoTime();
		Dataset<Row> recomendations = allRecommendations.filter("user_id="+userId).limit(1);
//		System.out.println("filtered: "+recomendations.count());
		long f1 = System.nanoTime();
		Utils.printTiming(f0, f1, "Filter time: ");

		return recomendations;
	}

	private void loadOrTrainModel() {
		if (debug) System.out.println("loadOrTrainModel");
		File f = new File(modelPath);
		if (f.exists()) {
			if (debug) System.out.println("Loading precomputed model");
			try {
				if (debug) System.out.println("trying to load model...");
				model = ALSModel.load(modelPath);
			} catch(Exception e) {
				System.out.println("Precomputed model is invalid --> regenerate");
				deletePreviousModel();
				train();
				saveTrainedModel();
			}
		} else {
			if (debug) System.out.println("No previously save model was found -> train and save one now...");
			train();
			saveTrainedModel();
		}
	}

	public static void deletePreviousModel() {
		File f = new File(modelPath);
		if (f.exists()) {
			Path rootPath = Paths.get(modelPath);
			try {
				Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile)
				.peek(System.out::println)
				.forEach(File::delete);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private void saveTrainedModel() {
		deletePreviousModel();
		try {
			model.save(modelPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private void train() {
		if (debug) System.out.println("Training model");
		long t0 = System.nanoTime();
		Dataset<Row> ratings = loadRatings();
		Dataset<Row>[] splits = ratings.randomSplit(new double[] {0.8, 0.2});
		Dataset<Row> training = splits[0];
		Dataset<Row> testing = splits[1];

		training.cache();
		testing.cache();

		// Build the recommendation model using ALS on the training data
		spark.sparkContext().setCheckpointDir("checkpoint/");
		ALS als = new ALS()
				.setImplicitPrefs(false)
				.setCheckpointInterval(8)
				.setNumBlocks(8)
				.setMaxIter(Config.numIterations)
				.setRank(5)
				.setNonnegative(true)
				.setRegParam(0.01)
				.setUserCol("user_id")
				.setItemCol("movie_id")
				.setRatingCol("rating");

		model = als.fit(training);

		long t1 = System.nanoTime();
		if(debug) Utils.printTiming(t0, t1, "Training time:");
		if (Config.shouldEvaluateModel) {
			if (debug) System.out.println("Evaluating model");
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
			long t2 = System.nanoTime();
			if (debug) Utils.printTiming(t1, t2, "Evaluation time:");
		}
	}
	
	private Dataset<Row> loadRatings() {
		return spark.read()
				.option("header", false)
				.schema(schemaRatings)
				.csv(ratingsFilePath)
				.persist(StorageLevel.MEMORY_AND_DISK());
	}

	private void initializeModel() {
		if (Config.forceRetrainModel) {
			deletePreviousModel();
			train();
			saveTrainedModel();
		} else {
			loadOrTrainModel();
		}
	}

	private void initializeSparkSession() {
		if (spark == null) {
			spark = SparkSession
					.builder()
					.appName("Spark Recommender (Mahout)")
					.master("local[*]")
					.config("spark.ui.enabled", "false")
					.config("spark.ui.showConsoleProgress", "false")
					.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.config("spark.executor.memory", "8g")
					.getOrCreate();
		}
	}

	private void configureLoggingOptions() {
		// Turn off unnecessary logging
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN);

	}

}
