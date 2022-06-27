package com.ukb.IGSB.TsvVcfUtils.vcf_rewriter;

import com.ukb.IGSB.TsvVcfUtils.init_db.GZIPfileInput;
import com.ukb.IGSB.TsvVcfUtils.vcf_rewriter.VcfRewriter;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class VcfRewriterTest {

  public static long filesCompareByByte(File f1, File f2) throws IOException {

    GZIPfileInput s1 = new GZIPfileInput();
    GZIPfileInput s2 = new GZIPfileInput();

    BufferedReader fis1 = s1.open(f1.toString());
    BufferedReader fis2 = s2.open(f2.toString());

    int ch = 0;
    long pos = 1;
    while ((ch = fis1.read()) != -1) {
      if (ch != fis2.read()) {
        return pos;
      }
      pos++;
    }
    if (fis2.read() == -1) {
      return -1;
    } else {
      return pos;
    }
  }

  @Test
  public void rewrite_fixed() {

    List<String> select = new ArrayList<String>();
    select.add("8|\\:|3:8|\\:|0:8|\\:|1:8|\\:|2:8|\\:|4");
    select.add("9|\\:|3:9|\\:|0:9|\\:|1:9|\\:|2:9|\\:|4");

    List<String> concat = new ArrayList<String>();
    concat.add("0|\\:|1|\\:|2|\\:|3|\\:|4");
    concat.add("0|\\:|1|\\:|2|\\:|3|\\:|4");

    List<Integer> to_rewrite = new ArrayList<Integer>();
    to_rewrite.add(8);
    to_rewrite.add(9);

    try {
      VcfRewriter r =
          new VcfRewriter(
              "test_data/rewrite_in.vcf.gz", "rewrite_out.vcf.gz", select, concat, to_rewrite);

      r.run();

      File firstFile = new File("rewrite_out.vcf.gz");
      File secondFile = new File("test_data/rewrite_out.vcf.gz");

      long comp = filesCompareByByte(firstFile, secondFile);

      Assert.assertEquals(comp, -1);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void resort() throws IOException {

    List<String> select = new ArrayList<String>();
    select.add("8|\\:|[GT]");
    select.add("8|\\:|[GT]");

    List<String> concat = new ArrayList<String>();
    concat.add("0|\\:|[~]");
    concat.add("0|\\:|[~]");

    List<Integer> rewrite = new ArrayList<Integer>();
    rewrite.add(8);
    rewrite.add(9);

    try {
      VcfRewriter r =
          new VcfRewriter(
              "test_data/rewrite_in_dynamic.vcf.gz",
              "rewrite_out_dynamic.vcf.bgz",
              select,
              concat,
              rewrite);

      r.run();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // We check that every single line of column GT 7 start with GT

    GZIPfileInput f = new GZIPfileInput();
    BufferedReader b = f.open("rewrite_out_dynamic.vcf.bgz");

    String line;
    while ((line = b.readLine()) != null) {
      if (line.startsWith("#")) {
        continue;
      }
      ;

      String[] ll = line.split("\t");

      Assert.assertEquals(ll[8].startsWith("GT"), true);
      Assert.assertEquals(
          ll[9].startsWith("0/0")
              || ll[9].startsWith("0/1")
              || ll[9].startsWith("1/0")
              || ll[9].startsWith("1/1")
              || ll[9].startsWith("1/2"),
          true);
    }

    File firstFile = new File("rewrite_out_dynamic.vcf.bgz");
    File secondFile = new File("test_data/rewrite_out_dynamic.vcf.bgz");

    long comp = filesCompareByByte(firstFile, secondFile);

    Assert.assertEquals(comp, -1);
  }
}
