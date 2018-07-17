package Spark_Mahout.spark_mahout;

import static spark.Spark.get;
import com.google.gson.Gson;

/**
 * Main class for running the server version of the recommender.
 * 
 * @author yoelluginbuhl
 *
 */
public class MainServer {
	public static void main(String[] args) {

		if (Config.parseArgs(args) == null) {
			System.err.println("Invalid configuration arguments.");
			System.exit(1);
		}
		Gson gson = new Gson();

		get("config",						RecommenderController.handlerConfig);
		get("test",							RecommenderController.handleTest);
		get("quit",							RecommenderController.handleQuit);

		get("recommendations/:id/:nb", 		RecommenderController.handleGetRecommendations,
				gson::toJson);
		get("anonymousRecommender/:id/:ids/:nb", RecommenderController.handleAnonymousRecommendations,
				gson::toJson);

		get("precomputeItemSimilarities", 	RecommenderController.handlePrecompute);
		get("mostSimilarItem/:id/:nb", 		RecommenderController.handleMostSimilarItems);
	}
}
