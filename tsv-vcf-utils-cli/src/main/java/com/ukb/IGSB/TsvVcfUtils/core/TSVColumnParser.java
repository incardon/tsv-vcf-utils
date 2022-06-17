package com.ukb.IGSB.TsvVcfUtils.core;

import com.ukb.IGSB.TsvVcfUtils.TsvVcfUtilsException;
import com.ukb.IGSB.TsvVcfUtils.init_db.GZIPfileInput;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TSVColumnParser {

  class StringHolder {
    String value;
  }

  List<Node> TsvColumnsInteger;
  List<Node> FormatInteger;

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

  private void parseColumnsArgsAndCreateNodesSimplified(
      String col_sel, String TsvFile, List<Node> ColumnsInteger)
      throws TsvVcfUtilsException, IOException {

    String[] first_split = col_sel.split(":");

    List<String> col_sel_ = new ArrayList<String>();
    col_sel_.add(new String());

    int k = 0;
    for (int i = 0; i < first_split.length - 1; i++) {
      if (!first_split[i].endsWith("\\")) {
        col_sel_.set(k, col_sel_.get(k) + first_split[i]);
        col_sel_.add(new String());
        k++;
      } else {
        col_sel_.set(
            k, col_sel_.get(k) + first_split[i].substring(0, first_split[i].length() - 1) + ":");
      }
    }
    col_sel_.set(k, col_sel_.get(k) + first_split[first_split.length - 1]);

    for (String intra_col_sel : col_sel_) {

      // Merge escaped

      String[] select_and_split = intra_col_sel.split("\\|");

      ColumnsInteger.add(new Node());
      Node j = ColumnsInteger.get(ColumnsInteger.size() - 1);

      for (int i = 0; i < select_and_split.length; i += 2) {
        if (i + 2 < select_and_split.length) {
          try {
            if (select_and_split[i].startsWith("[")) {
              j.id = -1;
              j.search = select_and_split[i].substring(1, select_and_split[i].length() - 1);
            } else {
              j.id = Integer.parseInt(select_and_split[i]);
            }
          } catch (NumberFormatException e) {
            throw new TsvVcfUtilsException(
                "Error: in line: "
                    + intra_col_sel
                    + " I was expecting element "
                    + i
                    + " splitted with | to be a number instead is "
                    + select_and_split[i]);
          }
          j.delimiter = select_and_split[i + 1];
          j.child = new Node();
          j = j.child;
        } else if (i < select_and_split.length) {
          try {
            if (select_and_split[i].startsWith("[")) {
              j.id = -1;
              j.search = select_and_split[i].substring(1, select_and_split[i].length() - 1);
            } else {
              j.id = Integer.parseInt(select_and_split[i]);
            }
          } catch (NumberFormatException e) {
            throw new TsvVcfUtilsException(
                "Error: in line: "
                    + intra_col_sel
                    + " I was expecting element "
                    + i
                    + " splitted with | to be a number instead is "
                    + select_and_split[i]);
          }
        } else {
          throw new TsvVcfUtilsException(
              "Error the Column selection sequence must terminate with a selector");
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

  public Boolean parseSimplified(String col, File TsvFile)
      throws TsvVcfUtilsException, IOException {

    TsvColumnsInteger = new ArrayList<Node>();

    parseColumnsArgsAndCreateNodesSimplified(col, TsvFile.toString(), TsvColumnsInteger);

    return true;
  }

  public Boolean setFormat(String format) {
    FormatInteger = new ArrayList<Node>();
    try {
      parseColumnsArgsAndCreateNodesSimplified(format, "unused", FormatInteger);
    } catch (TsvVcfUtilsException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return true;
  }

  public String getFormattedOutput(String[] arr) throws TsvVcfUtilsException {
    return getFormattedOutput(arr, -1);
  }

  public String getFormattedOutput(String[] arr, int c) throws TsvVcfUtilsException {
    List cols = new ArrayList<String>(TsvColumnsInteger.size());

    String fmt = new String();
    StringHolder other = new StringHolder();
    other.value = new String();

    int i = 0;
    for (Node j : TsvColumnsInteger) {
      cols.add(new String());

      if (j.child == null) {
        if (c < 0) {
          cols.set(i, arr[j.id]);
        } else {
          cols.set(i, arr[c]);
        }
      } else {
        if (c < 0) {
          cols.set(i, sub_node(j.child, j.delimiter, arr[j.id], arr[j.id], other));
        } else {
          cols.set(i, sub_node(j.child, j.delimiter, arr[j.id], arr[c], other));
        }
      }
      i++;
    }

    // Compose the rewrite

    i = 0;
    Node j = FormatInteger.get(0);

    while (j != null) {
      if (j.child == null) {
        fmt += cols.get(j.id);
      } else {
        StringHolder out = new StringHolder();
        out.value = new String();
        compose_node(j.child, j.delimiter, out, cols, other.value);
        if (j.id >= 0) {
          fmt += cols.get(j.id) + out.value;
        } else {
          fmt += out.value;
        }
      }

      i++;
      if (i < FormatInteger.size()) {
        j = FormatInteger.get(i);
      } else {
        j = null;
      }
    }

    return fmt;
  }

  private int find_id_in_list(String[] list, String str) {
    for (int i = 0; i < list.length; i++) {
      if (list[i].equals(str)) {
        return i;
      }
    }
    return -1;
  }

  private String sub_node(Node sub, String delimiter, String str, String str2, StringHolder other)
      throws TsvVcfUtilsException {

    if (sub.child != null) {
      try {
        String[] str_s = str.split(delimiter);
        String[] str2_s = str2.split(delimiter);
        if (str_s.length != 1) {
          return sub_node(sub.child, sub.delimiter, str_s[sub.id], str2_s[sub.id], other);
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
      if (sub.search != null) {
        sub.id = find_id_in_list(str.split(delimiter), sub.search);
      }

      if (sub.id < 0) {
        String[] spl = str.split(delimiter);

        for (int s = 0; s < spl.length - 1; s++) {
          other.value += spl[s] + delimiter;
        }
        other.value += spl[spl.length - 1];

        return "";
      }

      String[] spl = str2.split(delimiter);

      for (int s = 0; s < spl.length - 1; s++) {
        if (s != sub.id) {
          other.value += spl[s] + delimiter;
        }
      }
      if (sub.id != spl.length - 1) {
        other.value += spl[spl.length - 1];
      } else {
        other.value = other.value.substring(0, other.value.length() - 1);
      }

      return str2.split(delimiter)[sub.id];
    }
  }

  private void compose_node(
      Node sub, String delimiter, StringHolder composed, List<String> cols, String other)
      throws TsvVcfUtilsException {

    if (sub.child != null) {
      if (sub.id >= 0) {
        composed.value += delimiter + cols.get(sub.id);
      }

      compose_node(sub.child, sub.delimiter, composed, cols, other);
      return;
    }
    if (sub.id < 0 && sub.search.equals("~")) {
      composed.value += delimiter + other;
    } else {
      composed.value += delimiter + cols.get(sub.id);
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
        cols += sub_node(j.child, j.delimiter, arr[j.id], arr[j.id], new StringHolder()) + ";";
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
