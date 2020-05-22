# Item-based Recommender System

Recommender currently used by the [recommender website](https://github.com/katanlugi/recommender-website).

> This is a Maven based app which makes use of [apache spark](https://spark.apache.org/) mllib and [nd4j](https://nd4j.org/) as well as [Java Spark](http://sparkjava.com/) for the server part.

## Usage
This recommender can be used either in local or server mode running the ```Main.java``` or ```MainServer.java``` respectively. (You can also run multiple configuration in a row using the ```MainOptimalFinder.java``` which simply allows to execute multiple run, each time with other parameters).


### Main.java
Mainly used for debugging the core functionalities of the recommender.

Launch parameters:
```
... [-m n] [-i n] [-dbs s] [-dbn s] [-dbu s] [-dbp s] [-s b] [-v]
accepted parameters:
  -m n    n = number of iterations (default is 5)
  -nf n   n = number of features (default is 10)
  -i n    n = number of recommended items (default is 6)
  -dbs s  s = string for dbServer
  -dbn s  s = string for dbName
  -dbu s  s = string for dbUser
  -dbp s  s = string for dbPassword
  -src s  s = string for the file containing the ratings. Only when file based and not MySql!
  -s b    b = boolean for saving recommendations to db [true, false]
  -ft b   b = boolean for forcing a retrain of the model [true, false]
  -e b    b = boolean for evaluating the model [true, false]
  -im b   b = boolean for implicit preferences [true, false]
  -nn b   b = boolean for setNonNegative [true, false] best when inverse of -im
  -v      verbose (otherwise silent)
```

### MainServer.java
Used by the [demo website](https://github.com/katanlugi/recommender-website) as it will start a local server.

Launch parameters (same as for Main.java):
```
... [-m n] [-dbs s] [-dbn s] [-dbu s] [-dbp s] [-s b] [-v]
accepted parameters:
  -m n    n = number of iterations (default is 5)
  -nf n   n = number of features (default is 10)
  -dbs s  s = string for dbServer
  -dbn s  s = string for dbName
  -dbu s  s = string for dbUser
  -dbp s  s = string for dbPassword
  -src s  s = string for the file containing the ratings. Only when file based and not MySql!
  -s b    b = boolean for saving recommendations to db [true, false]
  -ft b   b = boolean for forcing a retrain of the model [true, false]
  -e b    b = boolean for evaluating the model [true, false]
  -im b   b = boolean for implicit preferences [true, false]
  -nn b   b = boolean for setNonNegative [true, false] best when inverse of -im
  -v      verbose (otherwise silent)
```

Once launched, you can test that the recommender is running by going on ```http://localhost:4567/test``` from your browser which should show you a "Hello, world!".

> To terminate the recommender server you can either terminate the process or go to ```http://localhost:4567/quit```

The possible variants are test, quit, recommendations and anonymousRecommender as shown below.
For "anonymous recommendations" you must provide a comma separated list of user preferences concisting of a movie id and a rating separated by ```:``` (movie_id1:rating1,movie_id2:rating2,...).

```
http://localhost:4567/test
http://localhost:4567/quit
http://localhost:4567/recommendations/[userId]/[howMany]
http://localhost:4567/anonymousRecommender/[userId]/[prefs]/[howMany]

example:
http://localhost:4567/anonymousRecommender/1000000/1:5.0,2:3.5,158:4.5,169:3.0,362:4.0,364:4.0,96030:0.5/5
```

The recommendations will be returned as json like:
```json
[
    {
        "userId": 1000000,
        "productId": 7210,
        "score": 0.06490040570497513
    },
    {
        "userId": 1000000,
        "productId": 3680,
        "score": 0.06687690317630768
    },
    {
        "userId": 1000000,
        "productId": 668,
        "score": 0.07041531801223755
    },
    {
        "userId": 1000000,
        "productId": 6918,
        "score": 0.07121540606021881
    },
    {
        "userId": 1000000,
        "productId": 3604,
        "score": 0.07831597328186035
    }
]
```

### MainOptimalFinder.java
Used in order to test different parameters combinations in order to train the "best" model.

No lauch parameters are required for this, just edit the String[] local_args at the begining of the main method with the desired parameters.

Then simply call ```trainAndEvaluateWithConfig(boolean implicit, boolean ratingsToConfScore, boolean setNonNegative, int numFeatures, int numIterations)``` with the desired values.

## Collaborative Filtering - spark.mllib
We make use of [spark.mllib](https://spark.apache.org/docs/preview/mllib-collaborative-filtering.html) for the recommendation which is why we use the [alternating least squares (ALS)][https://dl.acm.org/citation.cfm?id=1608614] alrogithm as spark.mllib is using it.

## Recommenders
Spark-mahout was built such that it should be quite easy to add more types of recommenders by implementing the interface ```SparkRecommender```. This interface contains the following methods:
```java
List<SparkMahoutRecommendation> recommend(long userID, int howMany);
List<SparkMahoutRecommendation> predict(long userID, int howMany);
List<SparkMahoutRecommendation> recommendToAnonymous(long anonymousID, int[] itemIDs, 
                                                double[] itemRatings, int howMany);
void precompute();	
void stop();
```

## Additionial readings
ALS is explained [here](https://spark.apache.org/docs/preview/ml-collaborative-filtering.html) and [here](https://bugra.github.io/work/notes/2014-04-19/alternating-least-squares-method-for-collaborative-filtering/)
###
