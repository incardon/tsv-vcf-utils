package com.ukb.IGSB.TsvVcfUtils.init_db;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class TestFiles {

  private List<String> TsvFiles;

  int margin;

  private List<Integer> formatInt;

  TestFiles(List<String> TsvFiles, String margin, List<String> format) throws IOException {
    this.TsvFiles = TsvFiles;
    this.margin = Integer.parseInt(margin);

    this.formatInt = new ArrayList<Integer>(TsvFiles.size());

    for (String col : format.get(0).split(":")) {
      formatInt.add(Integer.parseInt(col));
    }
  }

  void run() throws IOException {

    String[] lines = new String[margin];
    int pt = 0;

    int i = 0;
    for (String file : TsvFiles) {

      System.out.println("Processing " + file + "\n");

      InputStream fileStream = new FileInputStream(file);
      InputStream gzipStream =
          (file.endsWith(".gz") | file.endsWith(".bgz"))
              ? new GZIPInputStream(fileStream)
              : fileStream;
      Reader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
      BufferedReader buffered = new BufferedReader(decoder);

      FileOutputStream outputStream =
          new FileOutputStream(
              Paths.get(
                      file.substring(0, file.lastIndexOf('.'))
                          + "_test"
                          + file.substring(file.lastIndexOf('.')))
                  .getFileName()
                  .toString());
      GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
      Writer encoder = new OutputStreamWriter(gzipOutputStream, StandardCharsets.UTF_8);
      BufferedWriter bufferedWriter = new BufferedWriter(encoder);

      String line;
      String before_chr = "0";

      while ((line = buffered.readLine()) != null) {
        lines[pt] = line;

        String chromosome = line.split("\t")[formatInt.get(i)];
        if (!chromosome.equals(before_chr)) {
          // Go backward in the lines to find the valid line in this chromosome

          List<String> lines_copy = new ArrayList<>();
          int back = pt - 1;

          if (back < 0) {
            back = margin - 1;
          }

          //
          for (; back != pt; back--) {
            if (back < 0) {
              back = margin - 1;
            }

            String line_ = lines[back];

            if (line_ == null) {
              break;
            }

            if (before_chr == line_.split("\t")[formatInt.get(i)]) {
              lines_copy.add(0, lines[back]);
            }
          }

          // ok now we add the next lines
          if (lines[pt] != null) {
            lines_copy.add(lines[pt]);
          }

          for (int j = 0; j < margin; j++) {
            String line__ = buffered.readLine();

            if (line__ != null) {
              lines_copy.add(line__);
            } else {
              break;
            }
          }

          // write
          for (String line_here : lines_copy) {
            bufferedWriter.write(line_here + "\n");
          }
        }

        before_chr = chromosome;
        pt++;
        pt = pt % margin;
      }

      bufferedWriter.flush();
      encoder.flush();
      outputStream.flush();
      gzipOutputStream.flush();
      bufferedWriter.close();
      encoder.close();
      gzipOutputStream.close();
      outputStream.close();

      i++;
    }
  }
}
