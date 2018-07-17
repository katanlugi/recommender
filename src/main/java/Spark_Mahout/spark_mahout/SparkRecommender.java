package Spark_Mahout.spark_mahout;

import java.util.List;

/**
 * Interface for the required methods of a recommender.
 * 
 * @author yoelluginbuhl
 *
 */
public interface SparkRecommender {
	/**
	 * Computes the recommendations for a given userID.
	 * 
	 * @param userID		long	
	 * @param howMany	int		how many recommendations should be returned
	 * @return
	 */
	List<SparkMahoutRecommendation> recommend(long userID, int howMany);
	
	/**
	 * Computes the recommendations for an anonymous user.
	 * 
	 * @param anonymousID	long
	 * @param itemIDs		int[]		array containing the ids of the items
	 * @param itemRatings	double[]		array containing the rating of the items
	 * @param howMany		int			how many recommendations should be returned
	 * @return
	 */
	List<SparkMahoutRecommendation> recommendToAnonymous(long anonymousID, int[] itemIDs, double[] itemRatings, int howMany);
	
	/**
	 * Precomputes the model.
	 */
	void precompute();
	
	/**
	 * Terminate the application
	 */
	void stop();
	
	List<SparkMahoutRecommendation> predict(long userID, int howMany);
}
