package com.ukb.IGSB.TsvVcfUtils.init_db;

import htsjdk.samtools.util.BlockCompressedInputStream;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public class GZIPfileInput {

  InputStream fileStream;
  InputStream gzipStream;

  Reader decoder;

  public GZIPfileInput() {}

  public BufferedReader open(String file) throws IOException {

    try {
      fileStream = new FileInputStream(file);
      gzipStream =
          (file.endsWith(".gz") | file.endsWith(".bgz"))
              ? new BlockCompressedInputStream(fileStream)
              : fileStream;
      decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);

      BufferedReader br = new BufferedReader(decoder);

      String test = br.readLine();

      // Ok it work reopen

      fileStream = new FileInputStream(file);
      gzipStream =
          (file.endsWith(".gz") | file.endsWith(".bgz"))
              ? new BlockCompressedInputStream(fileStream)
              : fileStream;
      decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);

      br = new BufferedReader(decoder);

      return br;
    } catch (Exception e) {
      System.out.println("Openning with SAMtool failed, try standard GZIP");
    }

    // Ok it did not work reopen with GZIP

    fileStream = new FileInputStream(file);
    gzipStream =
        (file.endsWith(".gz") | file.endsWith(".bgz"))
            ? new GZIPInputStream(fileStream)
            : fileStream;
    decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);

    BufferedReader br = new BufferedReader(decoder);

    return br;
  }

  void close() throws IOException {
    decoder.close();
    gzipStream.close();
    fileStream.close();
  }
}
