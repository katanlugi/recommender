package Spark_Mahout.spark_mahout;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

public class MatrixFactorizationRecommenderTest {
	MatrixFactorizationRecommender mfr = null;
	@Before
	public void setUp() throws Exception {
		Config.forceRetrainModel = false;
		Config.shouldEvaluateModel = false;
		Config.shouldTransformRatingsToConfidenceScore = false;
		Config.ratingsFilePath = "data/small/ratings.csv";
		Config.numIterations = 1;
		Config.numFeatures = 1;
		mfr = new MatrixFactorizationRecommender();
	}

	@After
	public void tearDown() throws Exception {
		mfr = null;
	}

	@Test
	public void testConstructo() {
		MatrixFactorizationRecommender mfr2 = new MatrixFactorizationRecommender();
		assertNotNull(mfr2);
		assertTrue(mfr2 instanceof MatrixFactorizationRecommender);
		assertNotNull(MatrixFactorizationRecommender.sc);
		assertNotNull(MatrixFactorizationRecommender.spark);
	}

	@Test
	public void testRecommend() {
		long userId = 1;
		int howMany = 3;
		List<SparkMahoutRecommendation> smr = mfr.recommend(userId, howMany);
		Assert.assertEquals("Number of recommendation is wrong",  howMany, smr.size());
		Assert.assertEquals("Recommendation userId is wrong",  userId, smr.get(0).getUserId());
		
		smr = mfr.recommend(-1, 3);
		Assert.assertNotSame("Number of recommendation is wrong",  howMany, smr.size());
	}
	
	@Test
	public void testRecommendToAnonymous() {
		long userId = 2500000;
		int howMany = 3;
		int[] itemIDs = {1,2,3};
		double[] itemRatings = {5.0,2.0,4.0};
		List<SparkMahoutRecommendation> smr = mfr.recommendToAnonymous(userId,
				itemIDs, itemRatings, howMany);
		Assert.assertEquals("Number of recommendation is wrong",  howMany, smr.size());
		Assert.assertEquals("Recommendation userId is wrong",  userId, smr.get(0).getUserId());
		
		int[] wrongItemIDs = {1,2,3,5};
		double[] wrongItemRatings = {5.0,2.0,4.0};
		smr = mfr.recommendToAnonymous(userId,
				wrongItemIDs, wrongItemRatings, howMany);
		assertNull(smr);
		Assert.assertNull("Received recommendations but should have been null", smr);
	}
	
//	@Test
//	public void testRecommendToAnonymousNotConstant() {
//		Config.forceRetrainModel = true;
//		Config.ratingsFilePath = "data/big/ratings.csv";
//		Config.numIterations = 10;
//		Config.numFeatures = 10;
//		MatrixFactorizationRecommender mfr2 = new MatrixFactorizationRecommender();
//		long user1 = 3000000; // among others: DieHard, Waterworld, starwars, Leon, Schindler's list,...
//		long user2 = 3000001; // only kids movies (Disney, lions' king,...)
//		
//		int howMany = 6;
//		
//		int[] itemIDs1 = {1,2,13,34,313,364,588,594,595,596,1022,1023,1025,1029,1032,1538,1654};
//		double[] itemRatings1 = {5,5,4.5,5,5,5,4.5,4.5,4.5,4.5,4.5,4.5,4.5,5,4.5,4.5,4.5};
//		
//		int[] itemIDs2 = {1,165,208,260,293,527,858,1221,34198,41997,65216,92259};
//		double[] itemRatings2 = {5,3,4.5,5,5,4.5,4.5,4.5,5,5,5,3.5};
//		
//		List<SparkMahoutRecommendation> smr1 = mfr2.recommendToAnonymous(user1,
//				itemIDs1, itemRatings1, howMany);
//		
//		List<SparkMahoutRecommendation> smr2 = mfr2.recommendToAnonymous(user2,
//				itemIDs2, itemRatings2, howMany);
//		System.out.println("==+++===");
//		System.out.println(smr1);
//		System.out.println(smr2);
//		System.out.println("==+++===");
//		fail("TODO: implement test");
//	}
	
	@Test
	public void testPrecompute() {
		Config.ratingsFilePath = "data/small/ratings.csv";
		Config.numIterations = 1;
		Config.numFeatures = 1;
		mfr.precompute();
	}
	
	@Test
	public void testStop() {
		mfr.stop();
		assertNull(MatrixFactorizationRecommender.sc);
		assertNull(MatrixFactorizationRecommender.spark);
	}
	
	@Test
	public void testForceTrain() {
		Config.forceRetrainModel = true;
		Config.ratingsFilePath = "data/small/ratings.csv";
		mfr.precompute();
	}
	
	@Test
	public void testEvaluate() {
		Config.shouldEvaluateModel = true;
		Config.ratingsFilePath = "data/small/ratings.csv";
		mfr.precompute();
	}
	
	@Test
	public void testImplicitPrefs() {
		Config.forceRetrainModel = true;
		Config.ratingsFilePath = "data/small/ratings.csv";
		Config.implicitPrefs = true;
		mfr.precompute();
	}
	
	@Test
	public void testTransformRatingsToConfidenceScore() {
		Config.shouldTransformRatingsToConfidenceScore = true;
		Config.implicitPrefs = true;
		mfr.precompute();
	}
}
