package Spark_Mahout.spark_mahout;

/**
 * Configuration class allowing to have all the config option in one place.
 * 
 * @author yoelluginbuhl
 *
 */
public class Config {
	public static int numberOfRecommendations 	= 6;
	public static int numIterations 				= 5;
	public static int numFeatures				= 10;

	public static String ratingsFilePath = null;
	
	public static String dbServer 	= null;
	public static String dbUser 		= "[db_user]";
	public static String dbPassword 	= "[db_password]";
	public static String dbName 		= "[db_name]";

	public static String ratingsTable		= "ratings";
	public static String rtUserColumn 		= "user_id";
	public static String rtMovieColumn 		= "movie_id";
	public static String rtRatingColumn 		= "rating";
	public static String rtCreatedAtColumn	= "created_at";

	public static boolean saveToDB 			= false;
	public static boolean forceRetrainModel 	= false;
	public static boolean shouldEvaluateModel= false;
	public static boolean implicitPrefs		= false;
	public static boolean shouldTransformRatingsToConfidenceScore = false;
	public static boolean setNonNegative		= false;

	public static boolean print = false;

	@Override
	public String toString() {
		String str = "--Config--\n";
		str += "Project version: SparkMahout\n";
		str += "numIterations:\t"	+numIterations+"\n";
		str += "numFeatures:\t"		+numFeatures+"\n";
		str += "implicitPrefs:\t"	+implicitPrefs+"\n";
		str += "setNonNegative:\t"	+setNonNegative+"\n";
		str += "dbServer:\t"  		+dbServer+"\n";
		str += "dbName:\t\t"  		+dbName+"\n";
		str += "dbUser:\t\t"  		+dbUser+"\n";
		str += "dbPassword:\t"		+dbPassword+"\n";
		str += "saveToDB:\t"  		+saveToDB+"\n";
		str += "ratingsFilePath:\t" 	+ratingsFilePath+"\n";
		str += "forceRetrainModel:\t"+forceRetrainModel+"\n";
		str += "shouldEvaluateModel:\t"+shouldEvaluateModel+"\n";
		str += "# recommendations:\t"+numberOfRecommendations+"\n";
		return str;
	}
	
	public static String getConfig() {
		String str = "--Config--\n";
		str += "Project version: SparkMahout\n";
		str += "numIterations:\t"	+numIterations+"\n";
		str += "numFeatures:\t"		+numFeatures+"\n";
		str += "implicitPrefs:\t"	+implicitPrefs+"\n";
		str += "setNonNegative:\t"	+setNonNegative+"\n";
		str += "dbServer:\t"  		+dbServer+"\n";
		str += "dbName:\t\t"  		+dbName+"\n";
		str += "dbUser:\t\t"  		+dbUser+"\n";
		str += "dbPassword:\t"		+dbPassword+"\n";
		str += "saveToDB:\t"  		+saveToDB+"\n";
		str += "ratingsFilePath:\t" 	+ratingsFilePath+"\n";
		str += "forceRetrainModel:\t"+forceRetrainModel+"\n";
		str += "shouldEvaluateModel:\t"+shouldEvaluateModel+"\n";
		str += "# recommendations:\t"+numberOfRecommendations+"\n";
		return str;
	}
	
	public static void printConfig() {
		
		System.out.println(getConfig());
	}

	protected static void usage() {
		System.err.println("... [-m n] [-i n] [-dbs s] [-dbn s] [-dbu s] [-dbp s] [-s b] [-v]");
		System.err.println("You must either provide -src or the -db* parameters.");
		System.err.println("accepted parameters:");
		System.err.println("  -m n    n = number of iterations (default is 5)");
		System.err.println("  -nf n   n = number of features (default is 10)");
		System.err.println("  -i n    n = number of recommended items (default is 6)");
		System.err.println("  -dbs s  s = string for dbServer");
		System.err.println("  -dbn s  s = string for dbName");
		System.err.println("  -dbu s  s = string for dbUser");
		System.err.println("  -dbp s  s = string for dbPassword");
		System.err.println("  -src s  s = string for the file containing the ratings. Only when file based and not MySql!");
		System.err.println("  -s b    b = boolean for saving recommendations to db [true, false]");
		System.err.println("  -ft b   b = boolean for forcing a retrain of the model [true, false]");
		System.err.println("  -e b    b = boolean for evaluating the model [true, false]");
		System.err.println("  -im b   b = boolean for implicit preferences [true, false]");
		System.err.println("  -nn b   b = boolean for setNonNegative [true, false] best when inverse of -im");

		System.err.println("  -v      verbose (otherwise silent)");
	}

	/**
	 * Parse the argument's array into the config class
	 * 
	 * @param args String[]
	 * @return Config the configuration object initialized with the given args
	 */
	public static Config parseArgs(String[] args) {
		Config conf = new Config();

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-v")) {
				Config.print = true;
			}else if(args[i].equals("-m")) {
				i++;
				if (i < args.length) {
					try {
						Config.numIterations = Integer.parseInt(args[i]);
					}
					catch (NumberFormatException e) {
						System.err.println("-m requires a number");
						usage();
						return null;
//						System.exit(1);
					}
				}
			}else if(args[i].equals("-nf")) {
				i++;
				if (i < args.length) {
					try {
						Config.numFeatures = Integer.parseInt(args[i]);
					}
					catch (NumberFormatException e) {
						System.err.println("-nf requires a number");
						usage();
						return null;
//						System.exit(1);
					}
				}
			}else if(args[i].equals("-i")) {
				i++;
				if (i < args.length) {
					try {
						Config.numberOfRecommendations = Integer.parseInt(args[i]);
					}
					catch (NumberFormatException e) {
						System.err.println("-t requires a number");
						usage();
						return null;
//						System.exit(1);
					}
				}
			}else if(args[i].equals("-dbs")) {
				i++;
				if (i < args.length) {
					Config.dbServer = args[i];
				}
			}else if(args[i].equals("-dbn")) {
				i++;
				if (i < args.length) {
					Config.dbName = args[i];
				}
			}else if(args[i].equals("-dbu")) {
				i++;
				if (i < args.length) {
					Config.dbUser = args[i];
				}
			}else if(args[i].equals("-dbp")) {
				i++;
				if (i < args.length) {
					Config.dbPassword = args[i];
				}
			}else if(args[i].equals("-src")) {
				i++;
				if (i < args.length) {
					Config.ratingsFilePath = args[i];
				}
			}else if(args[i].equals("-s")) {
				i++;
				if (i < args.length) {
//					Config.saveToDB = Boolean.parseBoolean(args[i]);
					Config.saveToDB = Utils.parseBooleanString(args[i]);
				}
			}else if(args[i].equals("-ft")) {
				i++;
				if (i < args.length) {
					Config.forceRetrainModel = Utils.parseBooleanString(args[i]);//Boolean.parseBoolean(args[i]);
				}
			}else if(args[i].equals("-e")) {
				i++;
				if (i < args.length) {
					Config.shouldEvaluateModel = Utils.parseBooleanString(args[i]);//Boolean.parseBoolean(args[i]);
				}
			}else if(args[i].equals("-im")) {
				i++;
				if (i < args.length) {
					Config.implicitPrefs = Utils.parseBooleanString(args[i]);//Boolean.parseBoolean(args[i]);
				}
			}else if(args[i].equals("-nn")) {
				i++;
				if (i < args.length) {
					Config.setNonNegative = Utils.parseBooleanString(args[i]);//Boolean.parseBoolean(args[i]);
				}
			}
		}

		if (Config.print) System.out.println(conf);

		return conf;
	}

}
