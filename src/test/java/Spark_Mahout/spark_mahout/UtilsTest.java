package Spark_Mahout.spark_mahout;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import junit.framework.Assert;

public class UtilsTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetTopN() {
		int howMany = 3;
		INDArray matrix = Nd4j.zeros(10);
		INDArray ret = Utils.getTopN(matrix, howMany);
		
		Assert.assertEquals("getTopN return wrong number of values", howMany, ret.length());
		
		/*
		 * The following code was causing a wrong returned value.
		 * It is now fixed but we keep it just on case.
		 */
		long userId = 2500000;
		int[] itemIDs = {1,2,3};
		double[] itemRatings = {5.0,2.0,4.0};
		MatrixFactorizationRecommender mfr = new MatrixFactorizationRecommender();
		List<SparkMahoutRecommendation> smr = mfr.recommendToAnonymous(userId,
				itemIDs, itemRatings, howMany);
		System.out.println("smr.length: "+smr.size());
		Assert.assertEquals("getTopN return wrong number of values", howMany, smr.size());
	}

}
