package com.ukb.IGSB.TsvVcfUtils.core;

import com.ukb.IGSB.TsvVcfUtils.TsvVcfUtilsException;
import com.ukb.IGSB.TsvVcfUtils.core.Node;
import com.ukb.IGSB.TsvVcfUtils.core.TSVColumnParser;
import java.io.File;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TsvColumnParserTest {

  @Test
  public void check_construction_of_nodes_with_number() {

    TSVColumnParser tcp = new TSVColumnParser();

    try {
      tcp.parse("5", new File("Not-really-used"));
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Node n = tcp.getRoot();

    // A chain of 3 nodes

    Assert.assertEquals(n.id, 5);
    Assert.assertEquals(tcp.getNumberOfColumns(), 1);

    String line = new String("zero\tone\ttwo\tthree\tfour\tfive\tsix\tseven\teight");
    String[] l = line.split("\t");

    try {
      String cols = tcp.getColumns(l);
      Assert.assertEquals(cols, "five");
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    }

    Boolean t = tcp.setFormat("0");
    Assert.assertEquals(t, true);

    try {
      String ll = tcp.getFormattedOutput(l);
      Assert.assertEquals(ll, "five");
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void check_construction_of_nodes_with_number_and_delimiter() {

    TSVColumnParser tcp = new TSVColumnParser();

    try {
      tcp.parse("(7|AF=|(1|;|(0)))", new File("Not-really-used"));
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Node n = tcp.getRoot();

    // A chain of 3 nodes

    Assert.assertEquals(n.id, 7);
    Assert.assertEquals(n.delimiter, "AF=");

    n = n.child;

    Assert.assertEquals(n.id, 1);
    Assert.assertEquals(n.delimiter, ";");

    n = n.child;

    Assert.assertEquals(n.id, 0);

    // Check get column
    String line = new String("zero\tone\ttwo\tthree\tfour\tfive\tsix\tAF=7.0;AC=8.0\teight");
    String[] l = line.split("\t");

    try {
      String cols = tcp.getColumns(l);
      Assert.assertEquals(cols, "7.0");
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    }

    tcp.setFormat("-1|AF=|0");
    try {
      String out = tcp.getFormattedOutput(l);
      Assert.assertEquals(out, "AF=7.0");
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void check_construction_of_nodes_with_numbers() {

    TSVColumnParser tcp = new TSVColumnParser();

    try {
      tcp.parse("5:8:9", new File("Not-really-used"));
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Node n = tcp.getNode(0);
    Assert.assertEquals(n.id, 5);

    n = tcp.getNode(1);
    Assert.assertEquals(n.id, 8);

    n = tcp.getNode(2);
    Assert.assertEquals(n.id, 9);

    Assert.assertEquals(tcp.getNumberOfColumns(), 3);

    String line = new String("zero\tone\ttwo\tthree\tfour\tfive\tsix\tseven\teight\tnine");
    String[] l = line.split("\t");

    try {
      String cols = tcp.getColumns(l);
      Assert.assertEquals(cols, "five;eight;nine");
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    }

    tcp.setFormat("0|_|1|_|2");

    try {
      String col = tcp.getFormattedOutput(l);
      Assert.assertEquals(col, "five_eight_nine");
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void check_construction_of_nodes_with_numbers_and_delimiters() {

    TSVColumnParser tcp = new TSVColumnParser();

    try {
      tcp.parse(
          "(7|;|(4|=|(1))):(7|;|(5|=|(1))):(7|;|(6|=|(1))):(7|;|(7|=|(1)))",
          new File("Not-really-used"));
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    /////////////////////////////////////////////

    Node n = tcp.getRoot();

    // A chain of 3 nodes

    Assert.assertEquals(n.id, 7);
    Assert.assertEquals(n.delimiter, ";");

    n = n.child;

    Assert.assertEquals(n.id, 4);
    Assert.assertEquals(n.delimiter, "=");

    n = n.child;

    Assert.assertEquals(n.id, 1);

    ///////////////////////////////////////////////

    n = tcp.getNode(1);

    // A chain of 3 nodes

    Assert.assertEquals(n.id, 7);
    Assert.assertEquals(n.delimiter, ";");

    n = n.child;

    Assert.assertEquals(n.id, 5);
    Assert.assertEquals(n.delimiter, "=");

    n = n.child;

    Assert.assertEquals(n.id, 1);

    ///////////////////////////////////////////////////////

    n = tcp.getNode(2);

    // A chain of 3 nodes

    Assert.assertEquals(n.id, 7);
    Assert.assertEquals(n.delimiter, ";");

    n = n.child;

    Assert.assertEquals(n.id, 6);
    Assert.assertEquals(n.delimiter, "=");

    n = n.child;

    Assert.assertEquals(n.id, 1);

    ////////////////////////////////////////////////////////

    n = tcp.getNode(3);

    // A chain of 3 nodes

    Assert.assertEquals(n.id, 7);
    Assert.assertEquals(n.delimiter, ";");

    n = n.child;

    Assert.assertEquals(n.id, 7);
    Assert.assertEquals(n.delimiter, "=");

    n = n.child;

    Assert.assertEquals(n.id, 1);

    String line =
        new String(
            "zero\tone\ttwo\tthree\tfour\tfive\tsix\tzero=0.0;one=1.0;two=2.0;three=3.0;four=4.0;five=5.0;six=6.0;seven=7.0\teight");
    String[] l = line.split("\t");

    Assert.assertEquals(tcp.getNumberOfColumns(), 4);

    try {
      String cols = tcp.getColumns(l);
      Assert.assertEquals(cols, "4.0;5.0;6.0;7.0");
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    }

    tcp.setFormat("-1|sev=|3|\\:fiv=|1|\\:fou=|0");

    try {
      String cols = tcp.getFormattedOutput(l);
      Assert.assertEquals(cols, "sev=7.0:fiv=5.0:fou=4.0");
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void check_construction_of_nodes_simplified_dynamic() {

    TSVColumnParser tcp = new TSVColumnParser();

    try {
      tcp.parseSimplified("7|\\:|[GT]", new File("Not-really-used"));
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    /////////////////////////////////////////////

    Node n = tcp.getRoot();

    // A chain of 3 nodes

    Assert.assertEquals(n.id, 7);
    Assert.assertEquals(n.delimiter, ":");

    n = n.child;

    Assert.assertEquals(n.id, -1);
    Assert.assertEquals(n.search, "GT");

    tcp.setFormat("0|\\:|[~]");

    String line = new String("zero\tone\ttwo\tthree\tfour\tfive\tsix\tDB:GT:CT\t1:2:3");
    String[] l = line.split("\t");

    try {
      String cols = tcp.getFormattedOutput(l);
      Assert.assertEquals(cols, "GT:DB:CT");
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    }

    String line2 = new String("zero\tone\ttwo\tthree\tfour\tfive\tsix\tDB:CT:HP:GT\t1:2:3:4");
    String[] l2 = line2.split("\t");

    try {
      String cols = tcp.getFormattedOutput(l2);
      Assert.assertEquals(cols, "GT:DB:CT:HP");

      String cols2 = tcp.getFormattedOutput(l2, 8);

      Assert.assertEquals(cols2, "4:1:2:3");

    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void check_construction_file() {

    TSVColumnParser tcp = new TSVColumnParser();

    try {
      tcp.parse("banana:stop:chr(1-pos)", new File("test_data/test_tsv_tab"));
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Node n = tcp.getNode(0);
    Assert.assertEquals(n.id, 2);

    n = tcp.getNode(1);
    Assert.assertEquals(n.id, 4);

    n = tcp.getNode(2);
    Assert.assertEquals(n.id, 5);

    Assert.assertEquals(tcp.getNumberOfColumns(), 3);
    String line = new String("0.0\t1.0\t2.0\t3.0\t4.0\t5.0\t6.0\t7.0\t8.0");
    String[] arr = line.split("\t");

    try {
      String cols = tcp.getColumns(arr);
      Assert.assertEquals(cols, "2.0;4.0;5.0");
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    }
  }
}
