package com.ukb.IGSB.TsvVcfUtils.core;

import com.ukb.IGSB.TsvVcfUtils.TsvVcfUtilsException;
import com.ukb.IGSB.TsvVcfUtils.init_db.GZIPfileInput;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TSVColumnParser {

  List<Node> TsvColumnsInteger;

  /** Subparse the node * */
  private void sub_parse(Node sp, String str) {
    String[] id_str = str.split("\\|");
    sp.id = Integer.parseInt(id_str[0]);
    if (id_str.length <= 2) {
      return;
    }
    sp.delimiter = id_str[1];

    String str_tmp = str.substring(id_str[0].length() + id_str[1].length() + 2, str.length());

    if (str_tmp.startsWith("(")) {
      sp.child = new Node();
      sub_parse(sp.child, str_tmp.substring(1, str_tmp.length() - 1));
    }
  }

  private int findId(String file, String col_name) throws IOException, TsvVcfUtilsException {

    GZIPfileInput fileStream = new GZIPfileInput();
    BufferedReader buffered = fileStream.open(file);

    String line = buffered.readLine();

    if (!line.startsWith("#")) {
      throw new TsvVcfUtilsException("Invalid column " + col_name, new Exception());
    }

    String[] clist = line.substring(1).split("\t");

    for (int i = 0; i < clist.length; i++) {
      if (clist[i].equals(col_name)) {
        return i;
      }
    }

    throw new TsvVcfUtilsException("Invalid column " + col_name, new Exception());
  }

  private void parseColumnsArgsAndCreateNodes(
      String col_sel, String TsvFile, List<Node> ColumnsInteger)
      throws TsvVcfUtilsException, IOException {

    for (String intra_col_sel : col_sel.split(":")) {
      ColumnsInteger.add(new Node());
      if (intra_col_sel.startsWith("(")) {
        int lastNode_file = ColumnsInteger.size() - 1;
        sub_parse(
            ColumnsInteger.get(lastNode_file),
            intra_col_sel.substring(1, intra_col_sel.length() - 1));
      } else {
        try {
          int lastNode_file = ColumnsInteger.size() - 1;
          ColumnsInteger.get(lastNode_file).id = Integer.parseInt(intra_col_sel);
          ;
        } catch (NumberFormatException e) {
          // Must be a colums name
          int lastNode_file = ColumnsInteger.size() - 1;
          ColumnsInteger.get(lastNode_file).id = findId(TsvFile, intra_col_sel);
        }
      }
    }
  }

  public Node getRoot() {
    return TsvColumnsInteger.get(0);
  }

  public Node getNode(int i) {
    return TsvColumnsInteger.get(i);
  }

  public Boolean parse(String col, File TsvFile) throws TsvVcfUtilsException, IOException {

    TsvColumnsInteger = new ArrayList<Node>();

    parseColumnsArgsAndCreateNodes(col, TsvFile.toString(), TsvColumnsInteger);

    return true;
  }

  private String sub_node(Node sub, String delimiter, String str) throws TsvVcfUtilsException {

    if (sub.child != null) {
      try {
        String[] str_s = str.split(delimiter);
        if (str_s.length != 1) {
          return sub_node(sub.child, sub.delimiter, str_s[sub.id]);
        } else {
          return " ";
        }
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new TsvVcfUtilsException(
            "Error splitting "
                + delimiter
                + " and picking "
                + sub.id
                + " generate an error, check if the column exist of the id is correct",
            e);
      }
    } else {
      return str.split(delimiter)[sub.id];
    }
  }

  public String getColumns(String[] arr) throws TsvVcfUtilsException {
    String cols = new String();

    for (Node j : TsvColumnsInteger) {
      if (j.child == null) {
        if (arr[j.id].equals(".")) {
          cols += "null " + ";";
        } else {
          cols += arr[j.id].replace(";", " ") + ";";
        }
      } else {
        cols += sub_node(j.child, j.delimiter, arr[j.id]) + ";";
      }
    }

    return cols;
  }

  public String getNulls() {
    String nulls = new String();

    for (int j = 0; j < TsvColumnsInteger.size(); j++) {
      nulls += "null ;";
    }

    return nulls;
  }

  public int getNumberOfColumns() {
    return TsvColumnsInteger.size();
  }
}
