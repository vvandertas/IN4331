package com.vdtas;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.vdtas.helpers.ZipHelper.extractZip;

/**
 * @author vvandertas
 */
public class SqlApplication {
  private static final Logger log = LoggerFactory.getLogger(SqlApplication.class);
  private static SparkSession spark;
  private static String resultDir ;

  public static void main(String[] args) throws AnalysisException {
    String inputPath = args[0];
    String outputDir = args[1];
    String numberOfFolders = args[2];
    resultDir = args[3];

    // Create spark session and java context
    spark = SparkSession.builder().appName("spark-experiment").getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    // Download and extract input files to disc if they don't exist yet
    if(!new File(outputDir).exists() || new File(outputDir).listFiles().length < Integer.valueOf(numberOfFolders)) {
      log.info("No data found yet, extracting zip files..");
      new File(outputDir).mkdir();
      extractZip(inputPath + "*.zip", outputDir,4, jsc);
    }


    // Read the content of all the input csv files
    SQLContext sqc = new SQLContext(spark);
    Dataset<Row> songs = sqc.read().format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(outputDir + "*/*.csv");


    // Create a global temporary view
    songs.createGlobalTempView("songs");

    // queries
    fadeoutDurationWithTempoFilter();

    songsByDurationAndEndOfFadeIn();

    songsOrderedByNumSegments();

    numBars();
  }

  /**
   * All the distinct numbers of bars of songs, on a per artist basis
   */
  private static void numBars() {
    log.info("Finding the total number of bars per artist");
    long start = System.nanoTime();

    String barsQuery = "SELECT artistID, SUM(size(split(barsStart, ' '))) as numBars FROM global_temp.songs GROUP BY artistID";
    Dataset<Row> barsResult = spark.sql(barsQuery);
    long end = System.nanoTime();

    log.info("Finished querying for num bars, duration {} ms", (end - start) / 1000000);

    barsResult.show(10);
//    barsResult.javaRDD().saveAsTextFile(resultDir + "numBars");
  }

  /**
   * Get the duration of each fade-out (this is computed using: song_length - start_of_fade_out) for all songs having a temp above 100 and below 130
   */
  private static void songsByDurationAndEndOfFadeIn() {
    log.info("Finding all songs with a duration of 204 seconds and an end of fade in of less than 0.15 seconds");
    long start = System.nanoTime();

    String query = "SELECT * FROM global_temp.songs WHERE duration >= 204 AND duration < 205 AND endOfFadeIn < 0.15";
    Dataset<Row> result = spark.sql(query);

    long end = System.nanoTime();
    log.info("Finished querying for all songs by duration and end of fade in. Found {} songs,  duration: {} ms", result.count(), (end - start)/1000000);

    result.show(10);

    // write results to file
//    result.javaRDD().saveAsTextFile(resultDir + "songsByDuration");
  }

  /**
   * Retrieve all songs by artists who have released at least 10 songs before the year 1990, ordered by the number of segments of the song.
   */
  private static void songsOrderedByNumSegments() {
    log.info("Sorting songs by num segments, filtered by artists song count pre 1990");
    long start = System.nanoTime();

    String query = "SELECT * FROM  global_temp.songs WHERE " +
            "artistID IN " +
            "(SELECT artistID FROM global_temp.songs " +
            "WHERE year > 0 AND year < 1990 GROUP BY artistID HAVING count(*) >= 10) " +
            "ORDER BY size(split(segmentsStart, ' ')) DESC";


    Dataset<Row> result = spark.sql(query);
    long end = System.nanoTime();

    log.info("Finished sorting by segment. Found {} songs, duration {} ms", result.count(), (end - start) / 1000000);
    result.show(10);

    // write results to file
//    result.javaRDD().saveAsTextFile(resultDir + "songsBySegment");
  }

  /**
   * Retrieve all songs with a duration equal to 204s (or any random number), having an end_of_fade_in smaller that 0.15s
   */
  private static void fadeoutDurationWithTempoFilter() {
    log.info("Finding the fadeout duration of all songs with  a tempo between 100 and 130");
    long start = System.nanoTime();

    String query = "SELECT SongNumber, (duration - startOfFadeOut) as fadeoutDuration FROM global_temp.songs WHERE tempo BETWEEN 100 AND 130";
    Dataset<Row> result = spark.sql(query);

    long end = System.nanoTime();

    log.info("Finished querying for fadeout durations with tempo filter. Found {} songs, duration: {} ms", result.count(), (end - start)/1000000);
    result.show(10);

    // write results to file
//    result.javaRDD().saveAsTextFile(resultDir + "fadeouteResult");
  }

}