package Spark_Mahout.spark_mahout;

/**
 * Main class for running multiple runs with different configurations in a row.
 * You can use this in order to find the optimal configuration.
 * 
 * @author yoelluginbuhl
 *
 */
public class MainOptimalFinder {
	static MatrixFactorizationRecommender recommender;
	public static void main(String[] args) {
		String[] local_args = new String[] {
				"-dbs",  "[db_server]",
				"-dbn", "[db_name]",
				"-dbu", "[db_user]",
				"-dbp", "[db_password]",
				"-s", "false",
				"-ft", "true",
				"-e", "true"		// evaluate the trained model
//				"-m", "20",		// numIterations [default: 5] 20
//				"-nf", "10",		// numFeatures [default: 10] 10
//				"-im", "false",	// implicitPrefs [default: false]
//				"-v"
		};
		
		Config.parseArgs(local_args);
		System.out.println("impl:\tr2cs\tnonNeg\tnbF\tnbI\tMSE\t\t\tRMSE");
		/*
		 * ====== Impact of setNonNegative ============================
		 * only positive when implicit training true --> no need to change tweak it
		 */
//		trainAndEvaluateWithConfig(false, false,	false,	20,	10	); // WORST	RMSE=0.8213922026
//		trainAndEvaluateWithConfig(false, false,	true,	20,	10	); // BEST	RMSE=0.8127492146
//		trainAndEvaluateWithConfig(false, true,	false,	20,	10	);
//		trainAndEvaluateWithConfig(false, true,	true,	20,	10	);
		
//		trainAndEvaluateWithConfig(true,  false,	false,	20,	10	); // BEST	RMSE=2.787414957
//		trainAndEvaluateWithConfig(true,  false,	true,	20,	10	);
//		trainAndEvaluateWithConfig(true,  true,	false,	20,	10	);
//		trainAndEvaluateWithConfig(true,  true,	true,	20,	10	); // WoRST	RMSE=3.509545206
		
		/*
		 * =========Impact of latent factors ===========================
		 */
		trainAndEvaluateWithConfig(false, false,	true,	5,	10	);
//		trainAndEvaluateWithConfig(false, false,	true,	10,	10	);
//		trainAndEvaluateWithConfig(false, false,	true,	20,	10	);
//		trainAndEvaluateWithConfig(false, false,	true,	40,	10	);
//
//		trainAndEvaluateWithConfig(false, false,	true,	5,	20	);
//		trainAndEvaluateWithConfig(false, false,	true,	10,	20	);
//		trainAndEvaluateWithConfig(false, false,	true,	20,	20	);
//		trainAndEvaluateWithConfig(false, false,	true,	40,	20	);
////		---
//		trainAndEvaluateWithConfig(true, false,	false,	5,	10	);
//		trainAndEvaluateWithConfig(true, false,	false,	10,	10	);
//		trainAndEvaluateWithConfig(true, false,	false,	20,	10	);
//		trainAndEvaluateWithConfig(true, false,	false,	40,	10	);
//		
//		trainAndEvaluateWithConfig(true, false,	false,	5,	20	);
//		trainAndEvaluateWithConfig(true, false,	false,	10,	20	);
//		trainAndEvaluateWithConfig(true, false,	false,	20,	20	);
//		trainAndEvaluateWithConfig(true, false,	false,	40,	20	);
		
		
//		trainAndEvaluateWithConfig(false, false,	true,	8,	20	);
//		trainAndEvaluateWithConfig(false, false,	true,	12,	20	);
//		trainAndEvaluateWithConfig(false, false,	true,	15,	20	);
//		
//		trainAndEvaluateWithConfig(true, false,	false,	15,	20	);
//		trainAndEvaluateWithConfig(true, false,	false,	25,	20	);
//		trainAndEvaluateWithConfig(true, false,	false,	30,	20	);
	}
	
	static void trainAndEvaluateWithConfig(boolean implicit, boolean ratingsToConfScore,
			boolean setNonNegative, int numFeatures, int numIterations) {
		Config.implicitPrefs = implicit;
		Config.numFeatures = numFeatures;
		Config.numIterations = numIterations;
		Config.shouldTransformRatingsToConfidenceScore = ratingsToConfScore;
		Config.setNonNegative = setNonNegative;
		
		Config.ratingsFilePath = "data/big/ratings.csv";	// path of file containing the ratings

		recommender = new MatrixFactorizationRecommender();

		recommender.stop();
	}
}
