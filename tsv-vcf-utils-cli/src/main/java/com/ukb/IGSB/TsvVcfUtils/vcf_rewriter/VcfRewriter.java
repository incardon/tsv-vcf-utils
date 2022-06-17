package com.ukb.IGSB.TsvVcfUtils.vcf_rewriter;

import com.ukb.IGSB.TsvVcfUtils.TsvVcfUtilsException;
import com.ukb.IGSB.TsvVcfUtils.core.TSVColumnParser;
import com.ukb.IGSB.TsvVcfUtils.init_db.GZIPfileInput;
import com.ukb.IGSB.TsvVcfUtils.init_db.GZIPfileOutput;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class VcfRewriter {

  String VcfIN;
  String VcfOUT;

  List<TSVColumnParser> prs;
  List<Integer> to_rewrite;

  /**
   * Start a vcf database HTTP server
   *
   * @param VcfIN Path to the input Vcf
   * @param VcfOUT Path to the rewritten Vcf
   */
  public VcfRewriter(
      String VcfIN,
      String VcfOUT,
      List<String> select,
      List<String> concat,
      List<Integer> to_rewrite)
      throws Exception, IOException {

    this.VcfIN = VcfIN;
    if (VcfOUT == null) {
      VcfOUT = new String(VcfIN) + "_temp.vcf.gz";
    } else {
      this.VcfOUT = VcfOUT;
    }

    if (select == null) {
      select = new ArrayList<>();
    }
    if (concat == null) {
      concat = new ArrayList<>();
    }
    if (to_rewrite == null) {
      to_rewrite = new ArrayList<>();
    }

    if (select.size() != concat.size()) {
      throw new TsvVcfUtilsException(
          "Error select and concetanation rules must be the same in number");
    }

    prs = new ArrayList<TSVColumnParser>();
    for (int i = 0; i < select.size(); i++) {
      prs.add(new TSVColumnParser());
      prs.get(i).parseSimplified(select.get(i), new File("unused"));
      prs.get(i).setFormat(concat.get(i));
    }

    this.to_rewrite = to_rewrite;
  }

  private int is_cols_to_rewrite(int i) {

    for (int j = 0; j < this.to_rewrite.size(); j++) {
      if (i == this.to_rewrite.get(j)) {
        return j;
      }
    }
    return -1;
  }

  /** Execute TSV file import. */
  public void run() throws TsvVcfUtilsException, IOException {

    GZIPfileInput fileIN = new GZIPfileInput();
    BufferedReader bufferIN = fileIN.open(VcfIN);
    GZIPfileOutput fileOUT = new GZIPfileOutput();
    BufferedWriter bufferOUT = fileOUT.open(VcfOUT);

    String line;
    while ((line = bufferIN.readLine()) != null) {
      if (line.startsWith("#")) {
        bufferOUT.write(line + "\n");
        continue;
      }

      String rebuils_line = new String();
      String[] list_split = line.split("\t");

      for (int i = 0; i < list_split.length; i++) {
        int cr = is_cols_to_rewrite(i);
        if (cr >= 0) {
          rebuils_line += prs.get(cr).getFormattedOutput(list_split, i);
        } else {
          if (i == 7) {
            list_split[i] = list_split[i].replace(" ", "_");
          }

          rebuils_line += list_split[i];
        }

        if (i != list_split.length - 1) {
          rebuils_line += "\t";
        }
      }

      bufferOUT.write(rebuils_line + "\n");
    }

    bufferOUT.close();
  }
}
