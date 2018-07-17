package Spark_Mahout.spark_mahout;

import org.apache.spark.sql.Row;

/**
 * SparkMahoutRecommendation allows to combine the component of a recommendation into an object.
 * 
 * @author yoelluginbuhl
 *
 */
public class SparkMahoutRecommendation {
	
	private long userId;
	private int productId;
	private double score;
	
	/**
	 * Constructor from a mllib.recommendation.Rating object
	 * 
	 * @param rating org.apache.spark.mllib.recommendation.Rating
	 */
	public SparkMahoutRecommendation(org.apache.spark.mllib.recommendation.Rating rating) {
		this.userId = rating.user();
		this.productId = rating.product();
		this.score = rating.rating();
	}
	
	/**
	 * Constructor from a org.apache.spark.sql.Row object.
	 * 
	 * @param rating org.apache.spark.sql.Row
	 */
	public SparkMahoutRecommendation(Row rating) {
		this.userId = rating.getLong(0);
		this.productId = rating.getInt(1);
		this.score = rating.getDouble(2);
	}
	
	/**
	 * Constructor from a userId, productId and score
	 * 
	 * @param userId		long
	 * @param productId	int
	 * @param score		double
	 */
	public SparkMahoutRecommendation(long userId, int productId, double score) {
		this.userId = userId;
		this.productId = productId;
		this.score = score;
	}
	
	@Override
	public String toString() {
		return "("+userId+", "+productId+", "+score+")";
	}
	
	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public int getProductId() {
		return productId;
	}

	public void setProductId(int productId) {
		this.productId = productId;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double rating) {
		this.score = rating;
	}
}
