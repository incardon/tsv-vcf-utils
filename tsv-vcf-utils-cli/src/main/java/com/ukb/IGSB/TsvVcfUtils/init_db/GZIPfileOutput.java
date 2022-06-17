package com.ukb.IGSB.TsvVcfUtils.init_db;

import htsjdk.samtools.util.BlockCompressedOutputStream;
import java.io.*;
import java.nio.charset.StandardCharsets;

public class GZIPfileOutput {

  OutputStream fileStream;
  OutputStream gzipStream;

  Writer encoder;

  public GZIPfileOutput() {}

  public BufferedWriter open(String file) throws IOException {

    fileStream = new FileOutputStream(file);
    gzipStream =
        (file.endsWith(".gz") | file.endsWith(".bgz"))
            ? new BlockCompressedOutputStream(fileStream, new File(file))
            : fileStream;
    encoder = new OutputStreamWriter(gzipStream, StandardCharsets.UTF_8);

    BufferedWriter br = new BufferedWriter(encoder);

    return br;
  }

  void close() throws IOException {
    encoder.close();
    gzipStream.close();
    fileStream.close();
  }
}
