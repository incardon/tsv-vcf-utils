package com.ukb.IGSB.TsvVcfUtils.init_db;

import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.BlockCompressedOutputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
//import java.util.zip.BlockCompressedInputStream;
//import java.util.zip.BlockCompressedOutputStream;
import javax.swing.*;

public class ChromosomeSort {

  private List<String> TsvFiles;

  private List<Integer> formatInt;

  ChromosomeSort(List<String> TsvFiles, List<String> format) throws IOException {
    this.TsvFiles = TsvFiles;

    this.formatInt = new ArrayList<Integer>(TsvFiles.size());

    for (String col : format.get(0).split(":")) {
      formatInt.add(Integer.parseInt(col));
    }
  }

  void run() throws IOException {

    int pt = 0;

    int i = 0;
    for (String file : TsvFiles) {

      final long startTime = System.currentTimeMillis();

      System.out.println("Processing " + file + "\n");

      GZIPfileInput fileStream = new GZIPfileInput();
      BufferedReader buffered = fileStream.open(file);

      List<FileOutputStream> outputStream = new ArrayList<FileOutputStream>();
      List<BlockCompressedOutputStream> gzipOutputStream = new ArrayList<BlockCompressedOutputStream>();
      List<Writer> encoder = new ArrayList<Writer>();
      List<BufferedWriter> bufferedWriter = new ArrayList<BufferedWriter>();

      for (int j = 0; j < 25; j++) {
        File f = new File(Paths.get(
                        file.substring(0, file.lastIndexOf('.'))
                                + "_chr"
                                + Integer.toString(j)
                                + "_"
                                + file.substring(file.lastIndexOf('.')))
                .getFileName()
                .toString());

        outputStream.add(new FileOutputStream(f));

        gzipOutputStream.add(new BlockCompressedOutputStream(outputStream.get(j),f));
        encoder.add(new OutputStreamWriter(gzipOutputStream.get(j), StandardCharsets.UTF_8));
        bufferedWriter.add(new BufferedWriter(encoder.get(j)));
      }

      String line;

      int n_line_processed = 0;

      while ((line = buffered.readLine()) != null) {

        if (!line.startsWith("#")) {
          String chromosome = line.split("\t")[formatInt.get(i)];

          int chr = TsvVcfFileImporter.getChrInteger(chromosome) - 1;

          if (chr >= 0) {
            n_line_processed++;
            bufferedWriter.get(chr).write(line + "\n");
          }
        } else {
          n_line_processed++;
          bufferedWriter.get(0).write(line + "\n");
        }
      }

      System.out.println("Number of line processed: " + n_line_processed);

      buffered.close();

      for (int j = 0; j < 25; j++) {
        bufferedWriter.get(j).flush();
        encoder.get(j).flush();
        outputStream.get(j).flush();
        gzipOutputStream.get(j).flush();
        bufferedWriter.get(j).close();
        encoder.get(j).close();
//        gzipOutputStream.get(j).close();
        outputStream.get(j).close();
      }

      // We now assemble the output

      File f = new File(Paths.get(
                      file.substring(0, file.lastIndexOf('.'))
                              + "_sorted"
                              + file.substring(file.lastIndexOf('.')))
              .getFileName()
              .toString());

      FileOutputStream outputStream_ =
          new FileOutputStream(f);
      BlockCompressedOutputStream gzipOutputStream_ = new BlockCompressedOutputStream(outputStream_,f);
      Writer encoder_ = new OutputStreamWriter(gzipOutputStream_, StandardCharsets.UTF_8);
      BufferedWriter bufferedWriter_ = new BufferedWriter(encoder_);

      int lines_written = 0;

      for (int j = 0; j < 25; j++) {
        InputStream fileStream_ =
            new FileInputStream(
                Paths.get(
                        file.substring(0, file.lastIndexOf('.'))
                            + "_chr"
                            + Integer.toString(j)
                            + "_"
                            + file.substring(file.lastIndexOf('.')))
                    .getFileName()
                    .toString());
        InputStream gzipStream_ =
            (file.endsWith(".gz") | file.endsWith(".bgz"))
                ? new BlockCompressedInputStream(fileStream_)
                : fileStream_;
        Reader decoder_ = new InputStreamReader(gzipStream_, StandardCharsets.UTF_8);
        BufferedReader buffered_ = new BufferedReader(decoder_);

        while ((line = buffered_.readLine()) != null) {
          lines_written++;
          bufferedWriter_.write(line + "\n");
        }
      }

      System.out.println("Line written: " + lines_written);

      bufferedWriter_.flush();
      encoder_.flush();
      gzipOutputStream_.flush();
      outputStream_.flush();
      bufferedWriter_.close();
      encoder_.close();
//      gzipOutputStream_.close();
      outputStream_.close();

      for (int j = 0; j < 25; j++) {
        File f_ =
            new File(
                file.substring(0, file.lastIndexOf('.'))
                    + "_chr"
                    + Integer.toString(j)
                    + "_"
                    + file.substring(file.lastIndexOf('.')));
        f_.delete();
      }

      final long endTime = System.currentTimeMillis();

      System.out.println("Total execution time: " + (endTime - startTime) * 0.001 + " seconds");

      i++;
    }
  }
}
