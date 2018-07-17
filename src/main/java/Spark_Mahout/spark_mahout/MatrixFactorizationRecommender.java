package Spark_Mahout.spark_mahout;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import scala.Tuple2;

/**
 * MatrixFactorizationRecommender class implementing the SparkRecommender interface.
 * Recommender using alternating least squares (ALS).
 * 
 * @author yoelluginbuhl
 *
 */
public class MatrixFactorizationRecommender implements SparkRecommender, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 6155459233251944072L;

	private static String modelPath = Utils.getModelPath();

//	public static String ratingsSource = "mysql"; // "mysql" or "file"
	private static INDArray VtVtt;
	static SparkContext sc;

	private MatrixFactorizationModel model;

	//	private SparkSession spark;
	static SparkSession spark;

	/**
	 * Use this constructor if you want to load the data from a MySql server.
	 * The access config to the serve must be set using the Config.
	 */
	public MatrixFactorizationRecommender() {
		System.out.println("MatrixFactorizationRecommender()");
		System.out.println("MODEL PATH: "+modelPath);

		configureLoggingOptions();
		initializeSparkSession();
		initializeModel();
	}

	/**
	 * Use this constructor if you want to load the data from a file on disk
	 * 
	 * @param ratingsFilePath
	 * @deprecated use MatrixFactorizationRecommender() and the Config options instead
	 */
	public MatrixFactorizationRecommender(String ratingsFilePath) {
		System.out.println("MatrixFactorizationRecommender with ratin file");
		Config.ratingsFilePath = ratingsFilePath;
		configureLoggingOptions();
		initializeSparkSession();
		initializeModel();
	}

	@Override
	public void precompute() {
		System.out.println("MFR.precompute()...");
		forceRetrainModel();
	}

	@Override
	public List<SparkMahoutRecommendation> recommend(long userID, int howMany) {
		List<SparkMahoutRecommendation> smr = new ArrayList<>();
		Rating[] recommendations = new Rating[0];
		try {
			recommendations = model.recommendProducts((int) userID, howMany);
		} catch (NoSuchElementException e) {
			System.err.println("The given userId ("+userID+") is not in the trained model...");
			System.err.println("TODO: implement it currentlty simply skip it...");
		}
	
		for (Rating rating : recommendations) {
			smr.add(new SparkMahoutRecommendation(rating));
		}
		return smr;
	}

	@Override
	public List<SparkMahoutRecommendation> recommendToAnonymous(
			long anonymousID, int[] itemIDs, double[] itemRatings, int howMany) {
	
		if (itemIDs.length != itemRatings.length) {
			System.err.println("Inconsistant itemIDs and itemRatings sizes!");
			return null;
		}
		JavaPairRDD<Integer,double[]> productFeaturesRDD = Utils.massageToIntKey(model.productFeatures());
		List<Integer> pf_keys = productFeaturesRDD.sortByKey().keys().collect();
	
		INDArray matrix_u = Nd4j.zeros(pf_keys.size());
	
		//		itemRatings = transformRatingsToConfidenceScore(itemRatings);
	
		for (int i = 0; i < itemIDs.length; i++) {
			int itemId = itemIDs[i];
			int idx = pf_keys.indexOf(itemId);
			double itemRating = itemRatings[i];
	
			matrix_u.putScalar(idx, itemRating);
		}
	
		INDArray recommendations = matrix_u.mmul(VtVtt);
	
		List<SparkMahoutRecommendation> recoms = toSparkMahoutRecommendationList(
				recommendations,
				howMany,
				pf_keys,
				anonymousID
				);
	
		Comparator<SparkMahoutRecommendation> smrComparator = Comparator.comparingDouble(SparkMahoutRecommendation::getScore);
		recoms.sort(smrComparator);
		recoms = recoms.subList(0, howMany);
	
		return recoms;
	}

	@Override
	public void stop() {
		sc.stop();
		sc = null;
		spark.stop();
		spark = null;
	
		model = null;
		VtVtt = null;
	}

	//	@Override
	public List<SparkMahoutRecommendation> predict(long userID, int howMany) {
		return null;
	}

	/**
	 * Initializes the model either by loading it from 
	 * a precomputed one or by (re)training one.
	 */
	private void initializeModel() {
		if (Config.forceRetrainModel) {
			forceRetrainModel();
		} else {
			loadOrTrainModel();
		}
	}
	
	/**
	 * Turns off unnecessary logging options.
	 */
	private void configureLoggingOptions() {
		//		PropertyConfigurator.configure("data/log4j.properties");

		// Turn off unnecessary logging
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN);
		Logger.getLogger("spark.route.Routes").setLevel(Level.OFF);
		Logger.getRootLogger().setLevel(Level.OFF);
	}

	/**
	 * Initializes a spark session and spark context if not done so already.
	 */
	private void initializeSparkSession() {
		if (spark == null) {
			try {
				spark = SparkSession
						.builder()
						.appName("Spark Recommender (Mahout)")
						.master("local[*]")
						.config("spark.ui.enabled", "false")
						.config("spark.ui.showConsoleProgress", "false")
						.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
						.config("spark.executor.memory", "8g")
						.getOrCreate();
			} catch(Exception e) {
				e.printStackTrace();
				System.exit(0);
			} catch(Error e) {
				e.printStackTrace();
				System.exit(0);
			}
		}
		if (MatrixFactorizationRecommender.sc == null) {
			MatrixFactorizationRecommender.sc = spark.sparkContext();
		}
		if (Config.print) System.out.println("defaultParallelism: "+MatrixFactorizationRecommender.sc.defaultParallelism());
	}

	/**
	 * Forces the training of the model even if a pre-computed model is already saved.
	 */
	private void forceRetrainModel() {
		deletePreviousModel();
		trainAndSaveModel();
	}

	/**
	 * Train and save a model using ALS with the parameters set in Config.
	 */
	private void trainAndSaveModel() {
		try {
			/* Load the ratings */
			JavaRDD<Rating> ratings = loadRatings();

			if (Config.implicitPrefs) {
				ratings = transformRatingsToConfidenceScore(ratings);
			}

			/* Split the ratings into training and testing datasets */
			JavaRDD<Rating>[] splits = ratings.randomSplit(new double[] {0.8, 0.2}, 7856);
			JavaRDD<Rating> training = splits[0];
			JavaRDD<Rating> testing = splits[1];

			training.cache();
			testing.cache();

			/* train the model */
			ALS als = new ALS()
					/* auto-configure the # of block to parallelize the computation */
					.setBlocks(-1)
					.setImplicitPrefs(Config.implicitPrefs)
					.setRank(Config.numFeatures)
					/* only used when implicit. Defines how confident we are about the ratings */
					.setAlpha(100)
					.setIterations(Config.numIterations)
					/* non negative is good when implicit ratings */
					.setNonnegative(Config.setNonNegative)
					/* 0.01 give the best confidence it the trial runs performed up until now */
					.setLambda(0.01);
			
			/* Do the actual training */
			model = als.run(training);

			/* save the trained model */
			deletePreviousModel();
			model.save(sc, modelPath);

			if (Config.shouldEvaluateModel) {
				evaluateModel(testing);
			}

			/* Precomputes the matrix transpose used multiple time and keep it for future re-use */
			precomputeMatrixVtVtt(true);
		} catch(Exception e) {
			e.printStackTrace();
			stop();
			System.exit(0);
		} catch(Error err) {
			err.printStackTrace();
			stop();
			System.exit(0);
		}
	}

	/**
	 * Transforms the ratings [0-5] into confidence score [-2.5 - 2.5]
	 * such that they can be used with setImplicitPrefs=true.
	 * @param ratings JavaRDD<Rating>
	 * @return JavaRDD<Rating>
	 */
	private JavaRDD<Rating> transformRatingsToConfidenceScore(JavaRDD<Rating> ratings) {
		if(Config.shouldTransformRatingsToConfidenceScore) {
			ratings.foreach(r -> new Rating(r.user(), r.product(), r.rating() - 2.5));
		}

		return ratings;
	}

	/**
	 * Evaluate the trained model using the given testing dataset
	 * 
	 * @param testing	JavaRDD<Rating> testing dataset
	 */
	private void evaluateModel(JavaRDD<Rating> testing) {
		JavaRDD<Tuple2<Object, Object>> userProducts = testing.map(r -> new Tuple2<>(r.user(), r.product()));
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
				model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
				.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
				);
		JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD.fromJavaRDD(
				testing.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
				.join(predictions).values();

		double MSE = ratesAndPreds.mapToDouble(pair -> {
			//			double err = pair._1() - pair._2;
			double err = pair._1 - pair._2;
			return err * err;
		}).mean();
		double rmse = Math.sqrt(MSE);
		System.out.println(Config.implicitPrefs+"\t"
				+Config.shouldTransformRatingsToConfidenceScore+"\t"
				+Config.setNonNegative+"\t"
				+Config.numFeatures+"\t"
				+Config.numIterations+"\t"
				+MSE+"\t"
				+rmse);
		//		System.out.println("===========");
		//		System.out.println("impl:\t"+Config.implicitPrefs+" \tr2cs:\t"+Config.shouldTransformRatingsToConfidenceScore+"\tnonNeg:\t"+Config.setNonNegative+" \tnbF:\t"+Config.numFeatures+" \tnbI:\t"+Config.numIterations);
		//		System.out.println("MSE = \t"+ MSE +"\t RMSE = \t"+rmse);
	}

	/**
	 * Loads the ratings from either a file or a MySQL server depending on the value in Config.
	 * @return JavaRDD<Rating>
	 */
	private JavaRDD<Rating> loadRatings() {
		if (Config.ratingsFilePath != null) {
			if (Config.print) System.out.println("LoadRatings: from File");
			return loadRatingsFromFile();
		} else if (Config.dbServer != null) {
			if (Config.print) System.out.println("LoadRatings: fromMySQL");
			return loadRatingsFromMySQL();
		} else {
			System.err.println("Rating source currently not supported...");
			return null;
		}
	}

	/**
	 * Loads the ratings from a MySQL database using the params set in Config.
	 * @return JavaRDD<Rating>
	 */
	private JavaRDD<Rating> loadRatingsFromMySQL() {
		String jdbcUrl = "jdbc:mysql://"+Config.dbServer+":3306/"+Config.dbName+"?useUnicode=true"
				+ "&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";

		Properties connectionProperties = new Properties();
		connectionProperties.put("user", Config.dbUser);
		connectionProperties.put("password", Config.dbPassword);

		long t0 = System.nanoTime();
		Dataset<Row> ratings = spark.read().jdbc(jdbcUrl,
				"ratings",
				"user_id",
				1,
				1000001,
				256,
				connectionProperties
				);
		long t1 = System.nanoTime();
		Utils.printTiming(t0, t1, "Loading from JDBC: ");

		long t2 = System.nanoTime();
		Utils.printTiming(t1, t2, "ratings.show(): ");
		JavaRDD<Rating> rddRatings = null;
		try {
			rddRatings = ratings.toJavaRDD()
					.map(r -> new Rating(
							Integer.parseInt(r.getDecimal(0).toBigInteger().toString()),
							Integer.parseInt(r.getDecimal(1).toBigInteger().toString()),
							r.getDouble(2))
							);
			long t3 = System.nanoTime();
			Utils.printTiming(t2, t3, "rddRatings: ");
		} catch(Exception e) {
			e.printStackTrace();
			stop();
			System.exit(0);
		}

		return rddRatings;
	}
	
	/**
	 * Loads the ratings from a file
	 * @return JavaRDD<Rating>
	 */
	private JavaRDD<Rating> loadRatingsFromFile() {
		// Read user-item rating file. format - userId,itemId,rating
		JavaRDD<String> userItemRatingsFile = sc.textFile(Config.ratingsFilePath, 16).toJavaRDD();

		// Map file to Ratings(user,item,rating) tuples
		return userItemRatingsFile.map(new Function<String, Rating>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -8446071280842016902L;

			public Rating call(String s) {
				String[] sarray = s.split(",");
				return new Rating(Integer.parseInt(sarray[0]), Integer
						.parseInt(sarray[1]), Double.parseDouble(sarray[2]));
			}
		});
	}

	/**
	 * Loads or train a model if non pre-computed model are present.
	 */
	private void loadOrTrainModel() {
		File f = new File(modelPath);
		if (f.exists()) {
			System.out.println("Loading precomputed model");
			try {
				model = MatrixFactorizationModel.load(sc, modelPath);
				precomputeMatrixVtVtt(false);
			} catch (Exception e) {
				System.out.println("Precomputed model is invalid --> regenerate");
				deletePreviousModel();
				trainAndSaveModel();
			}

		} else {
			System.out.println("Training new model");
			trainAndSaveModel();
		}
	}

	/**
	 * Generates a List of SparkMahoutRecommendation from the given matrix.
	 * 
	 * @param matrix			recommendation matrix containing the scores for all products for a user.
	 * @param howMany		int how many recommendations should be retrieved
	 * @param pf_keys		List<Integer> product_features key list
	 * @param anonymousID	long userID of the user
	 * @return List<SparkMahoutRecommendation>	List of SparkMahoutRecommendation items.
	 */
	private List<SparkMahoutRecommendation> toSparkMahoutRecommendationList(
			INDArray matrix, int howMany, List<Integer> pf_keys, long anonymousID) {

		// worst recommendation from the top n recommendations (n: Config.numberOfRecommendations)
		double top_min = (double) Utils.getTopN(matrix, howMany).minNumber();

		List<SparkMahoutRecommendation> recommendations = new ArrayList<>();
		for (int i = 0; i < matrix.getRow(0).columns(); i++) {
			double tmp = matrix.getDouble(0, i);
			if (tmp >= top_min) {
				double value = matrix.getDouble(0, i);
				int movie_id = pf_keys.get(i);
				SparkMahoutRecommendation rec = new SparkMahoutRecommendation(anonymousID, movie_id, value);
				recommendations.add(rec);
			}
		}

		return recommendations;
	}

	/**
	 * Pre-computes the matrix VtVtt is needed. Vt it the latent factor matrix made of the productFeatures.
	 * Will only pre-compute it if it hasn't been done before or if we force it to do so.
	 * 
	 * @param force boolean allowing to force to execute the pre-computation.
	 */
	private void precomputeMatrixVtVtt(boolean force) {
		if (VtVtt == null || force) {
			INDArray Vt = createLatentMatrixVt();
			VtVtt = Vt.mmul(Vt.transpose());
		}
	}

	/**
	 * Generates the latent factor matrix Vt and returns it.
	 * 
	 * @return INDArray the matrix Vt
	 */
	private INDArray createLatentMatrixVt() {
		JavaPairRDD<Integer,double[]> itemFeaturesRDD = Utils.massageToIntKey(model.productFeatures());
		List<double[]> pf_vals = itemFeaturesRDD.sortByKey().values().collect();
		double[][] ddf_vals = Utils.toDoubleDoubleArray(pf_vals);

		INDArray Vt = Nd4j.create(ddf_vals);

		return Vt;
	}

	/**
	 * Deletes a potential previous pre-computed model if there is one.
	 */
	private void deletePreviousModel() {
		File dir = new File(modelPath);
		if (deleteDir(dir)) {
			System.out.println("deleted");
		}
	}
	
	/**
	 * Deletes the given File (file or directory)
	 * @param dir File
	 * @return boolean
	 */
	private boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
	        String[] children = dir.list();
	        for (int i = 0; i < children.length; i++) {
	            boolean success = deleteDir(new File(dir, children[i]));
	            if (!success) {
	                return false;
	            }
	        }
	    }

	    // The directory is now empty so delete it
	    return dir.delete();
	}
}
