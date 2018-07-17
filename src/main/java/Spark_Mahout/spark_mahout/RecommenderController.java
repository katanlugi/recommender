package Spark_Mahout.spark_mahout;

import java.util.List;

import spark.Request;
import spark.Response;
import spark.Route;

/**
 * RecommenderController class used to dispatch the 
 * different request of the Recommendation Server.
 * @author yoelluginbuhl
 *
 */
public class RecommenderController {
	//	public static Config conf;

	private static SparkRecommender recommender;

	public static Route handleTest = (Request req, Response res) -> {
		System.out.println("req: test");
		// TODO Auto-generated method stub
		return "Hello, world!";
	};
	
	/**
	 * Route to compute recommendations. The request takes in as arguments 
	 * a userId and the number of desired recommendations.
	 * 
	 * @return the recommendations in a json format
	 */
	public static Route handleGetRecommendations = (Request req, Response res) -> {
		System.out.println("req: handleGetRecommendations");
		setupThings();
		int userId = Integer.parseInt(req.params("id"));
		int nbItems = Integer.parseInt(req.params("nb"));
		List<SparkMahoutRecommendation> recommendations = recommender.recommend(userId, nbItems);
		res.type("application/json");
		return recommendations;
	};
	
	/**
	 * Route to precompute the model such that a call to handleGetRecommendations 
	 * won't have to first compute the model.
	 */
	public static Route handlePrecompute = (Request req, Response res) -> {
		System.out.println("in handlePrecompute");
		setupThings();
		recommender.precompute();
		return "precompute";
	};
	
	/**
	 * Route that computes recommendation for an anonymous user. The request takes in as 
	 * arguments a userId, the number of desired recommendations and a list of 
	 * ratings done by the anonymous user. This list is formated like 
	 * "itemId_1:rating_1,itemId2,rating2,..."
	 * 
	 * @return the recommendations in a json format
	 */
	public static Route handleAnonymousRecommendations = (Request req, Response res) -> {
		System.out.println("req: handleAnonymousRecommendations");
		setupThings();
		int userId = Integer.parseInt(req.params("id"));
		int howMany = Integer.parseInt(req.params("nb"));
		String[] prefs = req.params(":ids").split(",");
		int[] itemIDs = new int[prefs.length];
		double[] itemRatings = new double[prefs.length];
		
		int index = 0;
		for (String pref : prefs) {
			String[] vals = pref.split(":");
			itemIDs[index] = Integer.parseInt(vals[0]);
			itemRatings[index] = Double.parseDouble(vals[1]);
			index++;
		}
		
		List<SparkMahoutRecommendation> recommendations = recommender.recommendToAnonymous(
				userId,
				itemIDs,
				itemRatings,
				howMany
				);
		res.type("application/json");
		return recommendations;
	};

	/**
	 * Route to shut down the recommender server. Call this if the 
	 * recommender is started on a separate thread for example.
	 */
	public static Route handleQuit = (Request req, Response res) -> {
		System.out.println("req: quit");
		if (recommender != null) {
			recommender.stop();
		}
		System.exit(0);
		return "Over and done"; // just to keep the compiler happy
	};

	/**
	 * Route to compute the most similar items to a given item.
	 * Currently not implemented
	 */
	public static Route handleMostSimilarItems = (Request req, Response res) -> {
		// TODO Auto-generated method stub
		return "Not implemented yet...";
	};

	public static Route handlerConfig = (Request req, Response res) -> {
		return Config.getConfig();
	};

	/**
	 * Initialize the recommender with if it has not been yet.
	 */
	private static void setupThings() {
		System.out.println("in setupThings()");
		if (recommender == null) {
			if (Config.ratingsFilePath != null && Config.ratingsFilePath != "") {
				System.out.println("Initializing recommender = MatrixFactorizationRecommender "
						+ "with: "+Config.ratingsFilePath);
			}else {
				System.out.println("Initializing recommender = MatrixFactorizationRecommender");
			}
			recommender = new MatrixFactorizationRecommender();
			
			System.out.println("recommender should be initialized...");
		}
	}

}
