import sys
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType


# ---------------------------------
# Spark Session
# ---------------------------------
spark = SparkSession.builder.appName("MovieSimilaritiesDF").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------
# S3 paths
# ---------------------------------
MOVIES_PATH = "s3a://rev-spark-454497087304-us-east-2-an/ml-1m/movies.dat"
RATINGS_PATH = "s3a://rev-spark-454497087304-us-east-2-an/ml-1m/ratings.dat"

# schemas

moviesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True) \
                               ])
    
ratingsSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", DoubleType(), True), ])

# ---------------------------------
# Load movies
# ---------------------------------
movies = spark.read.schema(moviesSchema).option("sep","::").csv(MOVIES_PATH)


# ---------------------------------
# Load ratings
# ---------------------------------
ratings = spark.read.schema(ratingsSchema).option("sep","::").csv(RATINGS_PATH)
    


# ---------------------------------
# Self join by user
# ---------------------------------
r1 = ratings.alias("r1")
r2 = ratings.alias("r2")

moviePairs = (
    r1.join(
        r2,
        (f.col("r1.userID") == f.col("r2.userID")) &
        (f.col("r1.movieID") < f.col("r2.movieID"))
    )
    .select(
        f.col("r1.movieID").alias("movie1"),
        f.col("r2.movieID").alias("movie2"),
        f.col("r1.rating").alias("rating1"),
        f.col("r2.rating").alias("rating2")
    )
)

# ---------------------------------
# Compute cosine similarity
# ---------------------------------
movieSimilarities = (
    moviePairs
    .groupBy("movie1", "movie2")
    .agg(
        sum(f.col("rating1") * f.col("rating1")).alias("sum_xx"),
        sum(f.col("rating2") * f.col("rating2")).alias("sum_yy"),
        sum(f.col("rating1") * f.col("rating2")).alias("sum_xy"),
        count("*").alias("numPairs")
    )
    .withColumn(
        "score",
        f.col("sum_xy") /
        (f.sqrt(f.col("sum_xx")) * f.sqrt(f.col("sum_yy")))
    )
    .select(
        "movie1",
        "movie2",
        "score",
        "numPairs"
    )
)

movieSimilarities.cache()

# ---------------------------------
# Save results
# ---------------------------------
movieSimilarities.write.mode("overwrite").parquet(
    "s3a://rev-spark-454497087304-us-east-2-an/output/movie-sims-df"
)

# ---------------------------------
# Query similar movies
# ---------------------------------
if len(sys.argv) > 1:

    movieID = int(sys.argv[1])

    scoreThreshold = 0.97
    coOccurrenceThreshold = 50

    results = (
        movieSimilarities
        .filter(
            (
                (f.col("movie1") == movieID) |
                (f.col("movie2") == movieID)
            ) &
            (f.col("score") > scoreThreshold) &
            (f.col("numPairs") > coOccurrenceThreshold)
        )
    )

    similarMovies = (
        results
        .withColumn(
            "similarMovieID",
            f.col("movie2")
        )
        .withColumn(
            "similarMovieID",
            (
                (f.col("movie1") != movieID).cast("int") * f.col("movie1") +
                (f.col("movie1") == movieID).cast("int") * f.col("movie2")
            )
        )
    )

    finalResults = (
        similarMovies
        .join(
            f.broadcast(movies),
            similarMovies.similarMovieID == movies.movieID
        )
        .select(
            "title",
            "score",
            "numPairs"
        )
        .orderBy(f.col("score").desc())
        .limit(10)
    )

    print(f"\nTop 10 similar movies for movie ID {movieID}\n")

    finalResults.show(truncate=False)

spark.stop()
