package com.vdtas;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import java.io.File;
import java.util.*;

import static com.vdtas.helpers.FieldName.*;
import static com.vdtas.helpers.ZipHelper.extractZip;

/**
 * @author vvandertas
 */
public class Application {
  private static final Logger log = LoggerFactory.getLogger(Application.class);
  private static JavaRDD<Row> songsRDD;
  private static String resultDir;

  public static void main(String[] args) {
    String inputPath = args[0];
    String outputDir = args[1];
    String numberOfFolders = args[2];
    resultDir = args[3];

    // Create spark session
    SparkSession spark = SparkSession.builder().appName("spark-experiment").getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    // Download and extract input files to disk if they don't exist yet
    if(!new File(outputDir).exists() || new File(outputDir).listFiles().length < Integer.valueOf(numberOfFolders)) {
      log.info("No data found yet, extracting zip files..");
      new File(outputDir).mkdir();
      extractZip(inputPath + "*.zip", outputDir,4, jsc);
    }

    // Read the content of all the input csv files
    Dataset<Row> songs = spark.read().format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(outputDir + "*/*.csv");//.repartition(18); // Read all csv files at once

    // get an RDD with all song data
    songsRDD = songs.javaRDD();
    songsRDD.cache();

    // Queries
    averagePitchAndTimbre();

    fadeoutDurationWithTempoFilter();

    songsByDurationAndEndOfFadeIn();

    songsOrderedByNumSegments();

    numBars();
  }


  /**
   * Get the duration of each fade-out (this is computed using: song_length - start_of_fade_out) for all songs having a temp above 100 and below 130
   */
  private static void fadeoutDurationWithTempoFilter() {
    log.info("Finding the fadeout duration of all songs with  a tempo between 100 and 130");
    long start = System.nanoTime();
    JavaRDD<Map<Integer, Double>> result = songsRDD.filter(row -> {
      Double tempo = (Double) row.get(row.fieldIndex(TEMPO));
      Double duration = (Double) row.get(row.fieldIndex(DURATION));
      Double startOfFadeOut = (Double) row.get(row.fieldIndex(START_OF_FADE_OUT));

      // Filter out songs with duration or startOfFadeOut being null
      boolean nullValue = duration == null || startOfFadeOut == null;

      // and check the tempo condition
      return !nullValue && Double.compare(100d, tempo) > 0 && Double.compare(130d, tempo) < 0;
    }).map(song -> {
      Double duration = (Double) song.get(song.fieldIndex(DURATION));
      Double startOfFadeOut = (Double) song.get(song.fieldIndex(START_OF_FADE_OUT));

      return ImmutableMap.of((Integer) song.get(song.fieldIndex(SONG_NUMBER)), duration - startOfFadeOut);
    });

    long end = System.nanoTime();
    log.info("Finished querying for fadeout durations with tempo filter. Found {} songs, duration: {} ms", result.count(), (end - start)/1000000);

    // write results to file
//    result.saveAsTextFile(resultDir + "fadeouteResult");
  }

  /**
   * Retrieve all songs with a duration equal to 204s (or any random number), having an end_of_fade_in smaller that 0.15s
   */
  private static void songsByDurationAndEndOfFadeIn() {
    log.info("Finding all songs with a duration of 204 seconds and an end of fade in of less than 0.15 seconds");
    long start = System.nanoTime();

    // Find all songs with a duration of 204 seconds and an end_of_fade_in of less than 0.15s
    JavaRDD<Row> result = songsRDD.filter(row -> {
      Double duration = (Double) row.get(row.fieldIndex(DURATION));
      Double endOfFadeIn = (Double) row.get(row.fieldIndex(END_OF_FADE_IN));

      // Filter out songs with duration or startOfFadeOut being null
      boolean nullValue = duration == null || endOfFadeIn == null;

      return !nullValue && Double.compare(204d, duration) <= 0 && Double.compare(205d, duration) > 0 && Double.compare(0.15d, endOfFadeIn) > 0;
    });

    long end = System.nanoTime();
    log.info("Finished querying for all songs by duration and end of fade in. Found {} songs,  duration: {} ms", result.count(), (end - start)/1000000);

    // write results to file
//    result.saveAsTextFile(resultDir + "songsByDuration");
  }

  /**
   * Retrieve all songs by artists who have released at least 10 songs before the year 1990, ordered by the number of segments of the song.
   */
  private static void songsOrderedByNumSegments() {
    log.info("Finding all songs by artists who have released at least 10 songs before 1990, ordered by number of segments of the song. (DESC)");
    long start = System.nanoTime();

    // Filter by song year
    JavaRDD<Row> filteredSongs = songsRDD.filter(row -> (Integer) row.get(row.fieldIndex(YEAR)) < 1990 && (Integer) row.get(row.fieldIndex(YEAR))  != 0);

    // Group pre 1990 songs by artistID
    JavaPairRDD<String, Iterable<Row>> artistGroups = groupByArtist(filteredSongs);

    // Filter by count
    List<String> matchingArtistIds = artistGroups.filter(group -> Iterators.size(group._2.iterator()) >= 10).keys().collect();

    log.info("FOUND {} ARTISTS THAT MATCH", matchingArtistIds.size());

    // Filter all songs based on the matching artist ids
    JavaRDD<Row> result = songsRDD.filter(row -> {
      String artistId = (String) row.get(row.fieldIndex(ARTIST_ID));
      return StringUtils.isNotEmpty(artistId) && matchingArtistIds.contains(artistId);
    }).sortBy((Function<Row, Integer>) row ->
                    row.get(row.fieldIndex(SEGMENTS_START)) != null ? ((String) row.get(row.fieldIndex(SEGMENTS_START))).split("\\s+").length : 0
    , false, songsRDD.getNumPartitions()); // and sort by num segments

    long end = System.nanoTime();
    log.info("Finished querying for all songs by artists who have released at least 10 songs before 1990, ordered by number of segments of the song. Found {} songs, duration: {} ms", result.count(), (end - start)/1000000);

//    result.saveAsTextFile(resultDir + "songsBySegment");
  }

  /**
   * For songs between 1990 and 2000, find the average pitch and timbre per artist
   */
  private static void averagePitchAndTimbre() {
    log.info("Finding the average pitch and timbre per artist, for songs between 1990 and 2000");
    long start = System.nanoTime();

    // Filter by song year
    JavaRDD<Row> filteredSongs =
      songsRDD.filter(row -> 1990 <= (Integer) row.get(row.fieldIndex(YEAR)) && 2000 <= (Integer) row.get(row.fieldIndex(YEAR)));

    log.info("Found {} songs between 1990 && 2000", filteredSongs.count());

    Float[] initialPitches = new Float[12], initialTimbre = new Float[12];
    Arrays.fill(initialPitches, 0f);
    Arrays.fill(initialTimbre, 0f);

    JavaPairRDD<String, Tuple3<Integer, Float[], Float[]>> result = groupByArtist(filteredSongs).aggregateByKey(new Tuple3<>(0, initialPitches, initialTimbre),
      (averages, rowIterable) -> {
        int previousNumSongs = averages._1();
        int numSongs = Iterators.size(rowIterable.iterator());

        Float[] overallPitchAverage = averages._2();
        Float[] overallTimbreAverage = averages._3();

        for(Row row : rowIterable) {
          // split the String values containing the double array for timbre and pitch, resulting in String[] containing strings representing an array of size 12
          Float[] pitchAverage = extractAverageSegmentFeatures(row, SEGMENTS_PITCH);
          Float[] timbreAverage = extractAverageSegmentFeatures(row, SEGMENTS_TIMBRE);

          // add the song averages to the overall averages for this artist
          for(int i = 0; i < 12; i++) {
            overallPitchAverage[i] = overallPitchAverage[i] * (previousNumSongs / (previousNumSongs + numSongs)) + pitchAverage[i] * (numSongs / (previousNumSongs + numSongs));
            overallTimbreAverage[i] = overallTimbreAverage[i] * (previousNumSongs / (previousNumSongs + numSongs)) + timbreAverage[i] * (numSongs / (previousNumSongs + numSongs));
          }
        }

        return new Tuple3<>(numSongs + previousNumSongs, overallPitchAverage, overallTimbreAverage);
      },
      (v1, v2) -> {
        // create variables for the data we need
        Integer numSongs1 = v1._1(), numSongs2 = v2._1();
        Float[] pitchAverages1 = v1._2(), timbreAverages1 = v1._3(), pitchAverages2 = v2._2(), timbreAverages2 = v2._3();

        // combine the results from different machines by multiplying the averages by the fraction of the new number of songs seen for this group
        Float[] newPitchAverages =new Float[12], newTimbreAverages = new Float[12];
        Arrays.fill(newPitchAverages, 0f);
        Arrays.fill(newTimbreAverages, 0f);

        for(int i = 0; i < 12; i++) {
          newPitchAverages[i] = pitchAverages1[i] * (numSongs1 / (numSongs1 + numSongs2)) + pitchAverages2[i] * (numSongs2 / (numSongs1 + numSongs2));
          newTimbreAverages[i] = timbreAverages1[i] * (numSongs1 / (numSongs1 + numSongs2)) + timbreAverages2[i] * (numSongs2 / (numSongs1 + numSongs2));
        }
        // return the now number of songs, pitch averages and timbre averages for this group/artist
        return new Tuple3<>(numSongs1 + numSongs2, newPitchAverages, newTimbreAverages);
      });

    long end = System.nanoTime();
    log.info("Finished querying average pitch and timbre. Found {} artist, duration: {} ms", result.count(), (end - start)/1000000);

    // write results to file
//    result.map(groups -> {
//      String artistId = groups._1;
//      Tuple3<Integer, Float[], Float[]> values = groups._2;
//
//      HashMap<String, String> valueMap = new HashMap<>();
//      valueMap.put("ArtistId", artistId);
//      valueMap.put("pitch", Arrays.toString(values._2()));
//      valueMap.put("timbre", Arrays.toString(values._3()));
//
//      return valueMap;
//    }).saveAsTextFile(resultDir + "pitchTimbre");
  }

  /**
   *  Extract (from string to float) and average the segment features (pitch or timbre) for this song
   *
   * @param row   the song we are looking at
   * @param field either segments_pitch or segments_timbre
   * @return      float array containing the average of the 12 features
   */
  public static Float[] extractAverageSegmentFeatures(Row row, String field) {
    String pitchSegmentsString = (String) row.get(row.fieldIndex(field));
    log.info("Extracting average segment features");
    String[] pitchStringArrays = pitchSegmentsString.substring(2, pitchSegmentsString.length() - 2).split("\\]\\s\\[");
    log.info("Calculating average over {} segments", pitchStringArrays.length);
    return averageSegmentFeatures(pitchStringArrays);
  }


  /**
   * Converts the sting representations of the pitch/timbre values for each segment into floats and sums all of them.
   * @param inputArrays
   * @return float array containing the average of the 12 values relating to pitch or timbre for all segments of a song
   */
  public static Float[] averageSegmentFeatures(String[] inputArrays) {
//    log.info("calculation average segment values for {}", inputArrays);
    Float[] totals = new Float[12];
    Arrays.fill(totals, 0f);

    for(String inputArray : inputArrays) {
      // split the values for a segment and convert them to floats
      String[] array = inputArray.split("\\s+");

      // Convert string values to float, use 0 if a feature value is missing.
      Float[] segmentFeatures = Arrays.stream(array).map(feature -> StringUtils.isEmpty(feature) ? 0f : Float.valueOf(feature)).toArray(Float[]::new);

      // sum the values for each segment for this song.
        for(int i = 0; i < 12; i++) {
          totals[i] += segmentFeatures[i];
        }
    }
    // average the totals
    return Arrays.stream(totals).map(feature -> feature/inputArrays.length).toArray(Float[]::new);
  }


  /**
   * All the distinct numbers of bars of songs, on a per artist basis
   */
  private static void numBars() {
    log.info("Finding the total number of bars per artist");
    long start = System.nanoTime();

    JavaPairRDD<String, Integer> groupCountPair = groupByArtist(songsRDD).aggregateByKey(0,
            (total, rowIterable) -> {

              // Add the number of bars for each song to the total for this artist
              for(Row row : rowIterable) {
                total += row.get(row.fieldIndex(BARS_START)) != null ? ((String) row.get(row.fieldIndex(BARS_START))).split("\\s+").length : 0;
              }
              return total;
            }
            , (v1, v2) -> v1 + v2); // sum the totals of 2 groups

    long end = System.nanoTime();
    log.info("Finished querying for the total number of bars per artist. duration: {} ms", (end - start)/1000000);

//    groupCountPair.saveAsTextFile(resultDir + "numBars");

  }

  /**
   * Helper method to group a (pre-filtered) RDD by artist ID
   * @param baseRdd
   * @return
   */
  private static JavaPairRDD<String, Iterable<Row>> groupByArtist(JavaRDD<Row> baseRdd) {
    return baseRdd.groupBy(row -> (String) row.get(row.fieldIndex(ARTIST_ID)));
  }
}