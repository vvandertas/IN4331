package com.vdtas.helpers;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Helper created to extract the zip file(s) on the specified path
 * and place them in a folder with the same name as the zip file
 *
 * @author vvandertas
 */
public class ZipHelper {
  private static final Logger log = LoggerFactory.getLogger(ZipHelper.class);

  /**
   * Extract the zip file found @path and extract the data to @outputDir
   * @param path          input path to zip folder
   * @param outputDir     path to extract data to
   * @param minPartitions min number of spark partitions used to extract the binary data from the zip
   * @param jsc           the JavaSparkContext used
   */
  public static void extractZip(String path, String outputDir,  Integer minPartitions, JavaSparkContext jsc) {
    jsc.binaryFiles(path, minPartitions).foreach(tuple -> {
      log.info("String of tuple: {}", tuple._1);

      String[] split = tuple._1.split("/");
      String letter = split[split.length - 1].split("\\.")[0];

      log.info("Creating folder for {}", letter);
      new File(outputDir  +"/" + letter).mkdir();

      PortableDataStream content = tuple._2;
      try(ZipInputStream zis = new ZipInputStream(content.open())) {

        byte[] buffer = new byte[2048];
        ZipEntry nextEntry;

        // And write the content of each file in this zip to the new folder
        while((nextEntry = zis.getNextEntry()) != null) {
          log.info("Processing entry {}", nextEntry.getName());
          if(nextEntry.isDirectory()) {
            log.info("Found directory, continuing");
            continue;
          }

          // Make sure all files are added to the path [zip-name]/[file-name]
          String filePath = outputDir;
          if(!nextEntry.getName().contains(letter + "/")) {
            filePath += letter + "/";
          }
          filePath += nextEntry.getName();

          // skip files that have already been extracted
          log.info("Filepath: {}", filePath);
          if(new File(filePath).exists()){
            log.info("File already exists, continuing");
            continue;
          }

          // write the content to file
          try (FileOutputStream fos = new FileOutputStream(filePath)){
            int numBytes;
            while ((numBytes = zis.read(buffer)) > 0){
              fos.write(buffer, 0, numBytes);
            }
          }
        }
      }

      // and remove the zip after the last file has been processed.
      log.info("deleting zip {}", tuple._1.replace("file:", ""));
      new File(tuple._1.replace("file:", "")).delete();
    });
  }
}
