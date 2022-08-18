package com.ukb.IGSB.TsvVcfUtils.vcf_rewriter;

import com.ukb.IGSB.TsvVcfUtils.TsvVcfUtilsException;
import com.ukb.IGSB.TsvVcfUtils.core.TSVColumnParser;
import com.ukb.IGSB.TsvVcfUtils.init_db.GZIPfileInput;
import com.ukb.IGSB.TsvVcfUtils.init_db.GZIPfileOutput;
import java.io.*;
import java.util.*;

public class VcfRewriter {

  String VcfIN;
  String VcfOUT;

  List<TSVColumnParser> prs;
  List<Integer> to_rewrite;

  Boolean dp_ao_ad;

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
      List<Integer> to_rewrite,
      Boolean dp_ao_ad)
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
    this.dp_ao_ad = dp_ao_ad;
  }

  private int is_cols_to_rewrite(int i) {

    for (int j = 0; j < this.to_rewrite.size(); j++) {
      if (i == this.to_rewrite.get(j)) {
        return j;
      }
    }
    return -1;
  }

  private String remove_element(String str, int ele) {

    if (ele == -1) {
      return str;
    }

    List eles = new LinkedList<String>(Arrays.asList(str.split(":")));

    eles.remove(ele);

    return String.join(":", eles);
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

      TSVColumnParser gt_parser = new TSVColumnParser();
      TSVColumnParser dp_parser = new TSVColumnParser();
      TSVColumnParser ao_parser = new TSVColumnParser();
      TSVColumnParser ad_parser = new TSVColumnParser();
      gt_parser.parseSimplified("8|\\:|[GT]", new File("Not-really-used"));
      gt_parser.setFormat("0");
      dp_parser.parseSimplified("8|\\:|[DP]", new File("Not-really-used"));
      dp_parser.setFormat("0");
      ao_parser.parseSimplified("8|\\:|[AO]",new File("Not-really-used"));
      ao_parser.setFormat("0");
      ad_parser.parseSimplified("8|\\:|[AD]",new File("Not-really-used"));
      ad_parser.setFormat("0");

      if (dp_ao_ad == true) {
        String gt = gt_parser.getFormattedOutput(list_split,9);
        int dp = Integer.parseInt(dp_parser.getFormattedOutput(list_split,9));
        int ao = Integer.parseInt(ao_parser.getFormattedOutput(list_split, 9));
        String ad = ad_parser.getFormattedOutput(list_split,9);

        int gt_position = gt_parser.getRoot().child.id;
        int dp_position = dp_parser.getRoot().child.id;
        int ao_position = ao_parser.getRoot().child.id;
        int ad_position = ad_parser.getRoot().child.id;

        String rest = new String(list_split[8]);
        String rest_data = new String(list_split[9]);

        List<Integer> lList = new ArrayList<Integer>();

        lList.add(gt_position);
        lList.add(dp_position);
        lList.add(ao_position);
        lList.add(ad_position);

        Collections.sort(lList, Collections.reverseOrder());

        rest = remove_element(rest,lList.get(0));
        rest_data = remove_element(rest_data,lList.get(0));
        rest = remove_element(rest,lList.get(1));
        rest_data = remove_element(rest_data,lList.get(1));
        rest = remove_element(rest,lList.get(2));
        rest_data = remove_element(rest_data,lList.get(2));
        rest = remove_element(rest,lList.get(3));
        rest_data = remove_element(rest_data,lList.get(3));

        for (int i = 0; i < list_split.length; i++) {

          if (i == 8) {
            rebuils_line += new String("GT:DP:AO:AD:" + rest);
          }
          else if (i == 9) {
            rebuils_line += new String(gt + ":" + Integer.toString(dp) + ":" + Integer.toString(ao) + ":" + Integer.toString(dp-ao) + "," + Integer.toString(ao) + ":" + rest_data);
          } else {
            rebuils_line += list_split[i];
          }

          if (i != list_split.length - 1) {
            rebuils_line += "\t";
          }

        }

      }
      else {

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
      }

      bufferOUT.write(rebuils_line + "\n");
    }

    bufferOUT.close();
  }
}
