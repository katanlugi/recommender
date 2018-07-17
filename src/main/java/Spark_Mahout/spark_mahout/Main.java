package Spark_Mahout.spark_mahout;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Main class for running the recommender locally.
 * 
 * @author yoelluginbuhl
 *
 */
public class Main {
	
	public static void main(String[] args) {
		if (args.length > 0) {
			Config.parseArgs(args);
			mfrTest();
		} else {
			System.err.println("Missing launch parameters");
			Config.usage();
			System.exit(0);
		}
	}

	/**
	 * Tests the MatrixFactorizationRecommender using a mysql db for the ratings
	 */
	static void mfrTest() {
		System.out.println("------- Testing MatrixFactorizationRecommender with Config -------");
		long t0 = System.nanoTime();
		MatrixFactorizationRecommender recommender = new MatrixFactorizationRecommender();
		long t1 = System.nanoTime();
		Utils.printTiming(t0, t1, "MFR constructor: ");
		mfrRecommendation(recommender, 1);
		mfrRecommendation(recommender, 2);
		mfrRecommendation(recommender, 3);
		mfrRecommendation(recommender, 1000000);
		mfrAnonymousRecommendation(recommender, 1000000);
		recommender.stop();
	}

	/**
	 * Tests the MatrixFactorizationRecommender using a local file for the ratings
	 * @param ratingsFilePath
	 */
	static void mfrTest(String ratingsFilePath) {
		MatrixFactorizationRecommender recommender = null;
		try {
			System.out.println("------- Testing MatrixFactorizationRecommender -------");
			long t0 = System.nanoTime();
			Config.ratingsFilePath = ratingsFilePath;
			recommender = new MatrixFactorizationRecommender();
			long t1 = System.nanoTime();
			Utils.printTiming(t0, t1, "MFR constructor: ");
			mfrRecommendation(recommender, 1);
			mfrRecommendation(recommender, 2);
			mfrRecommendation(recommender, 3);
	
			mfrRecommendation(recommender, 1000000);
	
			mfrAnonymousRecommendation(recommender, 1000000);
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			recommender.stop();
		}		
	}

	/**
	 * Computes recommendations for an anonymous user using a MatrixFactorizationRecommender.
	 * @param recommender
	 * @param userId anonymous user id
	 */
	static void mfrAnonymousRecommendation(MatrixFactorizationRecommender recommender, int userId) {
		System.out.println("===== ANONYMOUS RECOMMENDATION =====");
		/*
		 * 1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
		 * 2,Jumanji (1995),Adventure|Children|Fantasy
		 * 158,Casper (1995),Adventure|Children
		 * 169,Free Willy 2: The Adventure Home (1995),Adventure|Children|Drama
		 * 362,"Jungle Book, The (1994)",Adventure|Children|Romance
		 * 364,"Lion King, The (1994)",Adventure|Animation|Children|Drama|Musical|IMAX
		 * 96030,"Weekend It Lives, The (Ax 'Em) (1992)",Horror
		 */
		int[] itemIDs 	     = {1,    2, 158, 169, 362, 364, 96030};
		double[] itemRatings = {5.0, 3.5, 4.5, 3.0, 4.0, 4.0, 0.5};
		long t0 = System.nanoTime();
		long anonymousID = -1;
		List<SparkMahoutRecommendation> recommendations = recommender.recommendToAnonymous(anonymousID, itemIDs, itemRatings, Config.numberOfRecommendations);
		Utils.printRatingRecommendations(recommendations);
		long t1 = System.nanoTime();
		Utils.printTiming(t0, t1, "Anonymous recommendation time: ");
	}

	/**
	 * Computes the recommendation for a user using a MatrixFactorizationRecommender.
	 * @param recommender
	 * @param userId
	 */
	static void mfrRecommendation(MatrixFactorizationRecommender recommender, int userId) {
		long t1 = System.nanoTime();
		List<SparkMahoutRecommendation> recommendations = recommender.recommend(userId, Config.numberOfRecommendations);
		Utils.printRatingRecommendations(recommendations);
		long t2 = System.nanoTime();
		Utils.printTiming(t1, t2, "Recommendation for user "+userId+":");
	}

	/**
	 * Non working test of a recommender based on mlib instead of mllib.
	 * @param ratingsFilePath
	 */
	static void mlEngineTest(String ratingsFilePath) {

		System.out.println("------- Testing ML Engine Recommender -------");
		long t0 = System.nanoTime();
		RecommendationEngine engine = new RecommendationEngine(ratingsFilePath);
		long t1 = System.nanoTime();
		Utils.printTiming(t0, t1, "MLEngine constructor: ");

		mlEngineRecommendation(engine, 1, 3);
		//		mlEngineRecommendation(engine, 2, 3);
		//		mlEngineRecommendation(engine, 3, 3);

		engine.stop();
		long t_end = System.nanoTime();
		Utils.printTiming(t0, t_end, "Total running time: ");
	}

	/**
	 * Non working test of a recommender based on mlib instead of mllib.
	 * @param ratingsFilePath
	 */
	static void mlEngineRecommendation(RecommendationEngine engine, int userId, int howMany) {
		long t1 = System.nanoTime();
		Dataset<Row> recomU1 = engine.recommend(userId, howMany);
		recomU1.show();
		long t2 = System.nanoTime();
		Utils.printTiming(t1, t2, "Recommendation for user "+userId+":");
	}
}
