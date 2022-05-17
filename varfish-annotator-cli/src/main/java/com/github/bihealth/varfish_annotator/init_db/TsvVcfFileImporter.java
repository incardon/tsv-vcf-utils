package com.github.bihealth.varfish_annotator.init_db;

import com.github.bihealth.varfish_annotator.VarfishAnnotatorException;
import com.github.bihealth.varfish_annotator.utils.UcscBinning;
import com.google.common.collect.ImmutableList;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class TsvVcfFileImporter {

  class VariantRow {
    public String chromosome;

    public int start;

    public int stop;

    public String Reference;

    public String Alternative;

    @Override
    public boolean equals(Object other) {
      if (other instanceof VariantRow) {
        VariantRow vr = (VariantRow) other;
        return (vr.chromosome.equals(chromosome)
            && vr.start == start
            && vr.stop == stop
            && vr.Reference.equals(Reference)
            && vr.Alternative.equals(Alternative));
      }

      return false;
    }

    public int hashCode() {
      return chromosome.hashCode() + start + stop + Reference.hashCode() + Alternative.hashCode();
    }
  };

  class VariantRowAnno {
    public String chromosome;

    public int start;

    public int stop;

    public String Reference;

    public String Alternative;

    public int bin;

    public String anno_data;
  };

  /** The name of the table in the database. */
  public static final String TABLE_NAME = "extra_annos";

  /** The expected TSV header. */
  public static ImmutableList<String> EXPECTED_HEADER =
      ImmutableList.of("release", "chromosome", "start", "end", "bin", "json");

  /** The JDBC connection. */
  private final Connection conn;

  protected final String refFastaPath;

  /** Path to Public TSV files */
  private List<String> TsvFiles;

  /** Columns selected for each file * */
  private final List<String> TsvColumns;

  /** Node for tree * */
  class Node {
    Node child;
    String delimiter;
    int id;
  }

  /** Tsv selected columns in integer * */
  private List<List<Node>> TsvColumnsInteger;

  /** Bed selected columns in integer * */
  private List<List<Node>> BedColumnsInteger;

  /** Path to Public TSV files */
  private List<String> VcfFiles;

  /** Colums selected for each file * */
  private final List<String> VcfColumns;

  /** It contain the last parsed variant row Tsv */
  private List<VariantRow> lastParsedLineTsv;

  /** Selected colums of the last parsed line in the TsvFile* */
  private List<String> lastParsedLineColumnsTsv;

  /** It contain the last parsed variant row Vcf */
  private List<VariantContext> lastParsedLineVcf;

  /** It contain the last parsed bed annotation * */
  private List<VariantRow> lastParsedLineBed;

  /** Selected colums of the last parsed line in the VcfFile* */
  private List<String> lastParsedLineColumnsVcf;

  /** Selected bed columns of the last parsed line in the Bed file * */
  private List<String> lastParsedLineColumnsBed;

  private String release;

  private List<Integer> vcfColumnNumbers;

  /** From where start chromosome start stop reference alternative * */
  List<Integer> start_cols;

  /** columns that does not follow the standard end here. * */
  List<List<Boolean>> bugged_columns_tsv;

  /** The format of each TSV file * */
  List<List<Integer>> formatInt;

  /** Bed files * */
  List<String> BedFiles;

  int actual_allele = 1;

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

  int findId(String file, String col_name) throws IOException, VarfishAnnotatorException {
    InputStream fileStream = new FileInputStream(file);
    InputStream gzipStream = file.endsWith(".gz") ? new GZIPInputStream(fileStream) : fileStream;
    Reader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
    BufferedReader buffered = new BufferedReader(decoder);

    String line = buffered.readLine();

    if (!line.startsWith("#")) {
      throw new VarfishAnnotatorException("Invalid column " + col_name, new Exception());
    }

    String[] clist = line.substring(1).split("\t");

    for (int i = 0; i < clist.length; i++) {
      if (clist[i].equals(col_name)) {
        return i;
      }
    }

    throw new VarfishAnnotatorException("Invalid column " + col_name, new Exception());
  }

  private void parseColumnsArgsAndCreateNodes(List<String> Columns, List<List<Node>> ColumnsInteger)
      throws VarfishAnnotatorException, IOException {

    for (String col_sel : Columns) {
      ColumnsInteger.add(new ArrayList<Node>());
      for (String intra_col_sel : col_sel.split(":")) {
        ColumnsInteger.get(ColumnsInteger.size() - 1).add(new Node());
        if (intra_col_sel.startsWith("(")) {
          int lastNode_file = ColumnsInteger.get(ColumnsInteger.size() - 1).size() - 1;
          sub_parse(
              ColumnsInteger.get(ColumnsInteger.size() - 1).get(lastNode_file),
              intra_col_sel.substring(1, intra_col_sel.length() - 1));
        } else {
          try {
            int lastNode_file = ColumnsInteger.get(ColumnsInteger.size() - 1).size() - 1;
            ColumnsInteger.get(ColumnsInteger.size() - 1).get(lastNode_file).id =
                Integer.parseInt(intra_col_sel);
            ;
          } catch (NumberFormatException e) {
            // Must be a colums name
            int lastNode_file = ColumnsInteger.get(ColumnsInteger.size() - 1).size() - 1;
            ColumnsInteger.get(ColumnsInteger.size() - 1).get(lastNode_file).id =
                findId(TsvFiles.get(ColumnsInteger.size() - 1), intra_col_sel);
          }
          // Integer.parseInt(intra_col_sel);
        }
      }
    }
  }

  /**
   * Construct the <tt>HgmdPublicImporter</tt> object.
   *
   * @param conn Connection to database
   * @param TsvFiles Paths to TSV files
   */
  public TsvVcfFileImporter(
      Connection conn,
      String refFastaPath,
      String release,
      List<String> TsvFiles,
      List<String> VcfFiles,
      List<String> BedFiles,
      List<String> TsvColumns,
      List<String> VcfColumns,
      List<String> BedColumns,
      List<String> format)
      throws VarfishAnnotatorException, IOException {
    this.conn = conn;
    this.TsvFiles = TsvFiles;
    this.VcfFiles = VcfFiles;
    this.BedFiles = BedFiles;
    this.TsvColumns = TsvColumns;
    this.VcfColumns = VcfColumns;
    this.refFastaPath = refFastaPath;
    this.release = release;

    if (TsvFiles == null) {
      this.TsvFiles = new ArrayList<String>();
    }

    if (VcfFiles == null) {
      this.VcfFiles = new ArrayList<String>();
    }

    if (BedFiles == null) {
      this.BedFiles = new ArrayList<String>();
    }

    lastParsedLineTsv = new ArrayList<VariantRow>(this.TsvFiles.size());
    lastParsedLineVcf = new ArrayList<VariantContext>(this.VcfFiles.size());
    lastParsedLineBed = new ArrayList<VariantRow>(this.BedFiles.size());

    this.start_cols = new ArrayList<Integer>(this.TsvFiles.size());
    for (Integer cl : this.start_cols) {
      cl = 0;
    }

    this.bugged_columns_tsv = new ArrayList<List<Boolean>>();

    for (int i = 0; i < TsvFiles.size(); i++) {
      this.bugged_columns_tsv.add(new ArrayList<Boolean>());

      for (int j = 0; j < 5; j++) {
        this.bugged_columns_tsv.get(i).add(Boolean.FALSE);
      }
    }

    if (TsvColumns.size() != TsvFiles.size()) {
      throw new VarfishAnnotatorException(
          "Error the you have "
              + TsvFiles.size()
              + " files, and I am expecting "
              + TsvFiles.size()
              + " columns selections, but i got "
              + TsvColumns.size(),
          new Exception(""));
    }

    TsvColumnsInteger = new ArrayList<>();
    parseColumnsArgsAndCreateNodes(TsvColumns, TsvColumnsInteger);

    lastParsedLineColumnsTsv = new ArrayList<>();

    for (int i = 0; i < TsvFiles.size(); i++) {
      lastParsedLineColumnsTsv.add(new String());
    }

    lastParsedLineColumnsVcf = new ArrayList<>();

    for (int i = 0; i < VcfFiles.size(); i++) {
      lastParsedLineColumnsVcf.add(new String());
    }

    lastParsedLineColumnsBed = new ArrayList<>();

    for (int i = 0; i < BedFiles.size(); i++) {
      lastParsedLineColumnsBed.add(new String());
    }

    List<String> valid_keywords = new ArrayList<String>();
    valid_keywords.add("chr");
    valid_keywords.add("start");
    valid_keywords.add("stop");
    valid_keywords.add("ref");
    valid_keywords.add("alt");

    if (format.size() != TsvFiles.size()) {
      throw new VarfishAnnotatorException(
          "Error you have specified --format for "
              + format.size()
              + " files but we have "
              + TsvFiles.size()
              + " tsv files",
          new Exception());
    }

    formatInt = new ArrayList<List<Integer>>();

    // We parse the format
    for (int i = 0; i < format.size(); i++) {
      formatInt.add(new ArrayList<Integer>(5));

      for (int j = 0; j < 5; j++) {
        formatInt.get(i).add(new Integer(0));
      }

      String fo = format.get(i);
      for (String fo_opt : fo.split(":")) {
        String[] equals = fo_opt.split("=");

        if (valid_keywords.contains(equals[0])) {
          int command = valid_keywords.indexOf(equals[0]);

          if (command < 5) {
            formatInt.get(i).set(command, Integer.parseInt(equals[1]));
          }
        } else {
          throw new VarfishAnnotatorException(
              "Invalid --format keyword: " + equals[0] + " is invalid", new Exception(""));
        }
      }
    }

    this.vcfColumnNumbers = new ArrayList<Integer>();
    for (String cols : this.VcfColumns) {
      this.vcfColumnNumbers.add(cols.split(":").length);
    }

    // Opening bedfiles

    BedColumnsInteger = new ArrayList<>();
    parseColumnsArgsAndCreateNodes(BedColumns, BedColumnsInteger);

    percentage = new double[chr_len.length + 1];

    tot_base = 0;
    for (int i = 0; i < percentage.length - 1; i++) {
      percentage[i] = tot_base;
      tot_base += chr_len[i];
    }
    percentage[percentage.length - 1] = tot_base;

    for (int i = 0; i < percentage.length - 1; i++) {
      percentage[i] = percentage[i] / tot_base * 100.0;
    }
    percentage[percentage.length - 1] = 100.0;
  }

  /** Execute TSV file import. */
  public void run() throws VarfishAnnotatorException {
    System.err.println("Re-creating table in database...");
    //    recreateTable();

    System.err.println("Importing Tsv file ...");

    importBedTsvVcfFiles();

    System.err.println("Done with importing Tsv files...");
  }

  /**
   * Re-create the HGMD Public table in the database.
   *
   * <p>After calling this method, the table has been created and is empty.
   */
  private void recreateTable() throws VarfishAnnotatorException {
    final String dropQuery = "DROP TABLE IF EXISTS " + TABLE_NAME;
    try (PreparedStatement stmt = conn.prepareStatement(dropQuery)) {
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with DROP TABLE statement", e);
    }

    final String createQuery =
        "CREATE TABLE "
            + TABLE_NAME
            + "("
            + "release VARCHAR(32) NOT NULL, "
            + "chromosome VARCHAR(32) NOT NULL, "
            + "start INTEGER NOT NULL, "
            + "end INTEGER NOT NULL, "
            + "bin INTEGER NOT NULL, "
            + "reference VARCHAR(512)"
            + "alternative VARCHAR(512)"
            + "anno_data jsonb NOT NULL"
            + ")";
    try (PreparedStatement stmt = conn.prepareStatement(createQuery)) {
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with CREATE TABLE statement", e);
    }

    final ImmutableList<String> indexQueries =
        ImmutableList.of(
            "CREATE PRIMARY KEY ON "
                + TABLE_NAME
                + " (release, chromosome, start, end, reference, alternative)",
            "CREATE INDEX ON "
                + TABLE_NAME
                + " (release, chromosome, start, end,teference, alternative)");
    for (String query : indexQueries) {
      try (PreparedStatement stmt = conn.prepareStatement(query)) {
        stmt.executeUpdate();
      } catch (SQLException e) {
        throw new VarfishAnnotatorException("Problem with CREATE INDEX statement", e);
      }
    }
  }

  /** Has a variant * */
  private boolean hasVariantTsv(int i, List<BufferedReader> files, int start) {
    if (lastParsedLineTsv.get(i) == null) {
      return false;
    }

    if (lastParsedLineTsv.get(i).start == start) {
      return true;
    }

    return false;
  }

  /** Has a variant * */
  private boolean hasVariantVcf(int i, List<CloseableIterator<VariantContext>> files, int start) {
    if (lastParsedLineVcf.get(i) == null) {
      return false;
    }

    if (lastParsedLineVcf.get(i).getStart() == start) {
      return true;
    }

    return false;
  }

  /** Has a variant * */
  private VariantRow getNextVariantTsv(int i, List<BufferedReader> files, StringHolder cols)
      throws IOException, VarfishAnnotatorException {
    VariantRow toReturn = lastParsedLineTsv.get(i);
    cols.value = getTsvColumns(i);

    lastParsedLineTsv.set(i, parseTsv(i, files));

    return toReturn;
  }

  private void getBedAnno(int i, List<BufferedReader> files, String cols, VariantRowAnno vr)
      throws IOException, VarfishAnnotatorException {

    VariantRow bed = lastParsedLineBed.get(i);

    if (vr.start < bed.start) {
      if (vr.stop < bed.start) {
        // No overlap rows with anno is smaller

        vr.anno_data += getBedNulls(i);
      } else {
        // overlap

        vr.anno_data += getBedColumns(i);
      }
    } else {
      if (vr.start < bed.stop) {
        // overlap

        vr.anno_data += getBedColumns(i);
      } else {
        // no overlap, next interval
        lastParsedLineBed.set(i, parseBed(i, files));

        vr.anno_data += getBedNulls(i);
      }
    }
  }

  /** Has a variant * */
  private VariantRow getNextVariantVcf(
      int i, List<CloseableIterator<VariantContext>> files, StringHolder cols) throws IOException {
    VariantContext vc = lastParsedLineVcf.get(i);

    VariantRow vr = new VariantRow();

    vr.chromosome = vc.getContig();
    vr.start = vc.getStart();
    vr.stop = vc.getEnd();
    vr.Reference = vc.getReference().getBaseString();
    vr.Alternative = vc.getAlleles().get(actual_allele).getBaseString();

    lastParsedLineColumnsVcf.set(i, new String());

    for (String col : VcfColumns.get(i).split(":")) {
      lastParsedLineColumnsVcf.set(
          i, lastParsedLineColumnsVcf.get(i) + vc.getAttributeAsString(col, "null") + ";");
    }

    cols.value = lastParsedLineColumnsVcf.get(i);

    actual_allele++;

    if (actual_allele == lastParsedLineVcf.get(i).getAlleles().size()) {
      parseVcf(i, files);
    }

    return vr;
  }

  String sub_node(Node sub, String delimiter, String str) throws VarfishAnnotatorException {

    if (sub.child != null) {
      try {
        String[] str_s = str.split(delimiter);
        if (str_s.length != 1) {
          return sub_node(sub.child, sub.delimiter, str_s[sub.id]);
        } else {
          return " ";
        }
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new VarfishAnnotatorException(
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

  /** ParseTsv * */
  VariantRow parseTsv(int i, List<BufferedReader> files)
      throws IOException, VarfishAnnotatorException {

    String line = files.get(i).readLine();
    if (line == null) {
      return null;
    }

    while (line.startsWith("#")) {
      line = files.get(i).readLine();
    }

    final ImmutableList<String> arr = ImmutableList.copyOf(line.split("\t"));

    VariantRow vr = new VariantRow();
    vr.chromosome = arr.get(formatInt.get(i).get(0));
    vr.start = Integer.parseInt(arr.get(formatInt.get(i).get(1)));

    if (this.bugged_columns_tsv.get(i).get(2) == true) {
      vr.stop = vr.start;
    } else {
      try {
        if (formatInt.get(i).get(2) != -1) {
          vr.stop = Integer.parseInt(arr.get(formatInt.get(i).get(2)));
        } else {
          vr.stop = vr.start;
        }
      } catch (NumberFormatException e) {
        this.bugged_columns_tsv.get(i).set(2, true);
        vr.stop = vr.start;
      }
    }

    vr.Reference = arr.get(formatInt.get(i).get(3));
    vr.Alternative = arr.get(formatInt.get(i).get(4));

    lastParsedLineColumnsTsv.set(i, new String());

    for (Node j : TsvColumnsInteger.get(i)) {
      if (j.child == null) {
        if (arr.get(j.id).equals(".")) {
          lastParsedLineColumnsTsv.set(i, lastParsedLineColumnsTsv.get(i) + "null " + ";");
        } else {
          lastParsedLineColumnsTsv.set(i, lastParsedLineColumnsTsv.get(i) + arr.get(j.id) + ";");
        }
      } else {
        lastParsedLineColumnsTsv.set(
            i,
            lastParsedLineColumnsTsv.get(i) + sub_node(j.child, j.delimiter, arr.get(j.id)) + ";");
      }
    }

    return vr;
  }

  int chr_len[] = {
    249250621, 243199373, 198022430, 191154276, 180915260, 171115067, 159138663, 146364022,
    141213431, 135534747, 135006516, 133851895, 115169878, 107349540, 102531392, 90354753, 81195210,
    78077248, 59128983, 63025520, 48129895, 51304566, 155270560, 5937356
  };

  long tot_base;

  double percentage[];

  double progress(String chr, int start) {
    int chr_int = 0;
    if (chr.equals("X")) {
      chr_int = 22;
    } else if (chr.equals("Y")) {
      chr_int = 23;
    } else if (chr.equals("MT")) {
      chr_int = 23;
    } else {
      chr_int = Integer.parseInt(chr) - 1;
    }

    return percentage[chr_int]
        + start / chr_len[chr_int] * (percentage[chr_int + 1] - percentage[chr_int]);
  }

  /** ParseVcf * */
  VariantRow parseVcf(int i, List<CloseableIterator<VariantContext>> files) throws IOException {

    actual_allele = 1;
    VariantRow vr = new VariantRow();
    VariantContext vc = files.get(i).next();
    lastParsedLineVcf.set(i, vc);
    if (vc == null) {
      return null;
    }
    vr.chromosome = vc.getContig();
    vr.start = vc.getStart();
    vr.stop = vc.getEnd();
    vr.Reference = vc.getReference().getBaseString();
    vr.Alternative = vc.getAlleles().get(actual_allele).getBaseString();

    lastParsedLineColumnsVcf.set(i, new String());

    for (String col : VcfColumns) {
      lastParsedLineColumnsVcf.set(
          i, lastParsedLineColumnsVcf.get(i) + vc.getAttributeAsString(col, "null") + ";");
    }

    return vr;
  }

  VariantRow parseBed(int i, List<BufferedReader> files)
      throws IOException, VarfishAnnotatorException {

    String line = files.get(i).readLine();
    if (line == null) {
      return null;
    }

    while (line.startsWith("#")) {
      line = files.get(i).readLine();
    }

    final ImmutableList<String> arr = ImmutableList.copyOf(line.split("\t"));

    VariantRow vr = new VariantRow();
    vr.chromosome = arr.get(0);
    vr.start = Integer.parseInt(arr.get(1));
    vr.stop = Integer.parseInt(arr.get(2));

    vr.Reference = "";
    vr.Alternative = "";

    lastParsedLineColumnsBed.set(i, new String());

    for (Node j : BedColumnsInteger.get(i)) {
      if (j.child == null) {
        try {
          if (arr.get(j.id).equals(".")) {
            lastParsedLineColumnsBed.set(i, lastParsedLineColumnsBed.get(i) + "null " + ";");
          } else {
            lastParsedLineColumnsBed.set(i, lastParsedLineColumnsBed.get(i) + arr.get(j.id) + ";");
          }
        } catch (Exception e) {
          throw new VarfishAnnotatorException(
              "Error column " + j.id + " does not exist in the bed file");
        }
      } else {
        lastParsedLineColumnsBed.set(
            i,
            lastParsedLineColumnsBed.get(i) + sub_node(j.child, j.delimiter, arr.get(j.id)) + ";");
      }
    }

    return vr;
  }

  /** Get selected Tsv colums * */
  String getTsvColumns(int i) {
    return lastParsedLineColumnsTsv.get(i);
  }

  /** Get selected Tsv colums * */
  String getVcfColumns(int i) {
    return lastParsedLineColumnsVcf.get(i);
  }

  String getBedColumns(int i) {
    return lastParsedLineColumnsBed.get(i);
  }

  class StringHolder {
    public String value;
  }

  class IntHolder {
    public Integer value;

    IntHolder() {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  class Pair {
    int id;
    String data;

    Pair(int id, String data) {
      this.id = id;
      this.data = data;
    }
  }

  String getTsvNulls(int i) {
    String nulls = new String();

    for (int j = 0; j < TsvColumnsInteger.get(i).size(); j++) {
      nulls += "null ;";
    }

    return nulls;
  }

  String getVcfNulls(int i) {
    String nulls = new String();

    for (int j = 0; j < this.vcfColumnNumbers.get(i); j++) {
      nulls += "null ;";
    }

    return nulls;
  }

  String getBedNulls(int i) {
    String nulls = new String();

    for (int j = 0; j < BedColumnsInteger.get(i).size(); j++) {
      nulls += "null ;";
    }

    return nulls;
  }

  /** Create rows * */
  private List<VariantRowAnno> create_rows(
      IntHolder start,
      List<BufferedReader> tsvFiles,
      List<CloseableIterator<VariantContext>> VcfStreams,
      List<BufferedReader> BedStreams /*,
      VariantNormalizer normalizer*/)
      throws IOException, VarfishAnnotatorException {

    // rows to merge
    HashMap<VariantRow, List<Pair>> rows = new HashMap<VariantRow, List<Pair>>();

    StringHolder cols = new StringHolder();

    for (int i = 0; i < tsvFiles.size(); i++) {

      while (hasVariantTsv(i, tsvFiles, start.value)) {
        VariantRow vr = getNextVariantTsv(i, tsvFiles, cols);

        String alt_full = vr.Alternative;
        // Sometimes alternatives are listed with a comma
        for (String alt_s : alt_full.split(",")) {

          VariantRow vr_s = new VariantRow();
          vr_s.chromosome = vr.chromosome;
          vr_s.start = vr.start;
          vr_s.stop = vr.stop;
          vr_s.Reference = vr.Reference;
          vr_s.Alternative = alt_s;

          if (!rows.containsKey(vr_s)) {
            rows.put(vr_s, new ArrayList<Pair>());
          }

          List<Pair> r = rows.get(vr_s);
          r.add(new Pair(i, cols.value));
        }
      }
    }

    for (int i = 0; i < VcfStreams.size(); i++) {

      while (hasVariantVcf(i, VcfStreams, start.value)) {

        VariantRow vr = getNextVariantVcf(i, VcfStreams, cols);

        if (!rows.containsKey(vr)) {
          rows.put(vr, new ArrayList<Pair>());
        }

        List<Pair> r = rows.get(vr);
        r.add(new Pair(-(i + 1), cols.value));
      }
    }

    List<VariantRowAnno> rowsWithAnnoData = new ArrayList<VariantRowAnno>();
    for (Map.Entry<VariantRow, List<Pair>> entry : rows.entrySet()) {

      VariantRowAnno vra = new VariantRowAnno();

      vra.chromosome = entry.getKey().chromosome;
      vra.start = entry.getKey().start;
      vra.stop = entry.getKey().stop;
      vra.Reference = entry.getKey().Reference;
      vra.Alternative = entry.getKey().Alternative;
      vra.bin = UcscBinning.getContainingBin(vra.start, vra.start);

      String cols_data = new String();
      List<Pair> lp = entry.getValue();

      int j = 0;
      for (int i = 0; i < TsvFiles.size(); i++) {
        if (j < lp.size() && lp.get(j).id == i) {
          cols_data += lp.get(j).data;
          j++;
        } else {
          cols_data += getTsvNulls(i);
        }
      }

      j = 0;
      for (int i = 0; i < VcfFiles.size(); i++) {
        if (j < lp.size() && Math.abs(lp.get(j).id + 1) == i) {
          cols_data += lp.get(j).data;
          j++;
        } else {
          cols_data += getVcfNulls(i);
        }
      }

      vra.anno_data = cols_data;

      rowsWithAnnoData.add(vra);
    }

    int start_ = 1000000000;

    // calculate next start
    for (VariantRow vr : lastParsedLineTsv) {
      if (vr != null && vr.start < start_) {
        start_ = vr.start;
      }
    }

    for (VariantContext vc : lastParsedLineVcf) {
      if (vc != null && vc.getStart() < start_) {
        start_ = vc.getStart();
      }
    }

    start.value = start_;

    for (int j = 0; j < rowsWithAnnoData.size(); j++) {

      for (int i = 0; i < BedStreams.size(); i++) {
        getBedAnno(i, BedStreams, cols.value, rowsWithAnnoData.get(j));
      }
    }

    return rowsWithAnnoData;
  }

  private boolean hasNextTsvOrVcf(
      List<BufferedReader> TsvStreams, List<CloseableIterator<VariantContext>> VcfStreamsIt) {
    boolean tv = false;

    for (int i = 0; i < TsvStreams.size(); i++) {
      tv |= lastParsedLineTsv.get(i) != null;
    }

    for (int i = 0; i < VcfStreamsIt.size(); i++) {
      tv |= lastParsedLineVcf.get(i) != null;
    }

    return tv;
  }

  private BufferedReader createStreamFromFilename(String pathFile)
      throws VarfishAnnotatorException {
    try {
      InputStream fileStream = new FileInputStream(pathFile);
      InputStream gzipStream =
          (pathFile.endsWith(".gz") | pathFile.endsWith(".bgz"))
              ? new GZIPInputStream(fileStream)
              : fileStream;
      Reader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
      BufferedReader buffered = new BufferedReader(decoder);

      return buffered;
    } catch (IOException e) {
      throw new VarfishAnnotatorException("Problem reading gziped TSV file", e);
    }
  }

  /** Import the TSV database file */
  private void importBedTsvVcfFiles() throws VarfishAnnotatorException {

    // Creates a FileWriter
    FileWriter file = null;
    try {
      file = new FileWriter("ExtraAnnos.tsv");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    BufferedWriter buffer = new BufferedWriter(file);

    // final VariantNormalizer normalizer = new VariantNormalizer(refFastaPath);

    /*        final String insertQuery =
    "MERGE INTO "
            + TABLE_NAME
            + " (release, chromosome, start, end, reference, alternative, anno_data)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?)";*/

    List<BufferedReader> TsvStreams = new ArrayList<BufferedReader>();
    List<BufferedReader> BedStreams = new ArrayList<BufferedReader>();
    List<VCFFileReader> VcfStreams = new ArrayList<VCFFileReader>();
    List<CloseableIterator<VariantContext>> VcfStreamsIt =
        new ArrayList<CloseableIterator<VariantContext>>();

    for (String pathTsvFile : TsvFiles) {
      System.err.println("Opening TSV: file:" + pathTsvFile);

      TsvStreams.add(createStreamFromFilename(pathTsvFile));
    }

    for (String pathVcfFile : VcfFiles) {
      System.err.println("Opening VCF: file:" + pathVcfFile);

      VcfStreams.add(new VCFFileReader(new File(pathVcfFile), true));
      VcfStreamsIt.add(VcfStreams.get(VcfStreams.size() - 1).iterator());
    }

    for (String pathBedFile : BedFiles) {
      System.err.println("Opening BED: file:" + pathBedFile);

      BedStreams.add(createStreamFromFilename(pathBedFile));
    }

    int chromosome = 100;
    IntHolder start = new IntHolder();
    start.value = 1000000000;

    // Get the first line for each filegetAttributeAsString
    try {

      for (int i = 0; i < TsvStreams.size(); i++) {
        lastParsedLineTsv.add(parseTsv(i, TsvStreams));
        if (lastParsedLineTsv.get(i).start < start.value) {
          start.value = lastParsedLineTsv.get(i).start;
        }
      }

      for (int i = 0; i < VcfStreamsIt.size(); i++) {
        lastParsedLineVcf.add(VcfStreamsIt.get(i).next());
        if (lastParsedLineVcf.get(i).getStart() < start.value) {
          start.value = lastParsedLineVcf.get(i).getStart();
        }
      }

      for (int i = 0; i < BedStreams.size(); i++) {
        lastParsedLineBed.add(parseBed(i, BedStreams));
        if (lastParsedLineBed.get(i).start < start.value) {
          start.value = lastParsedLineBed.get(i).start;
        }
      }

      // }

      double p_display = 0.0;
      int nit = 0;
      while (hasNextTsvOrVcf(TsvStreams, VcfStreamsIt)) {

        List<VariantRowAnno> annos_data =
            create_rows(start, TsvStreams, VcfStreamsIt, BedStreams /*, normalizer*/);

        for (VariantRowAnno va : annos_data) {

          String row = new String();
          row += "GRCh37\t";
          row += va.chromosome + "\t";
          row += va.start + "\t";
          row += va.stop + "\t";
          row += va.Reference + "\t";
          row += va.Alternative + "\t";
          row += va.bin + "\t";
          row += "[" + va.anno_data + "]" + "\n";

          buffer.write(row);
        }

        if (nit % 100 == 0 && annos_data.size() != 0) {
          double p = progress(annos_data.get(0).chromosome, start.value);
          if ((p - p_display * 100) >= 0.1) {
            p_display = p;
            System.out.println("Progress: " + Double.toString(p_display));
          }
        }
        nit++;
      }

      buffer.close();
      /*            final PreparedStatement stmt = conn.prepareStatement(insertQuery);

      for (VariantRowAnno va : annos_data) {
          stmt.setString(1, "GRCh37");
          stmt.setString(2, va.chromosome);
          stmt.setInt(3, va.start);
          stmt.setInt(4, va.stop);
          stmt.setString(5, va.Alternative);
          stmt.setString(6, va.Reference);
          stmt.executeUpdate();
          stmt.close();
      }*/

      // Write on file the table

    } catch (IOException e) {
      throw new VarfishAnnotatorException("Problem reading gziped TSV file", e);
    }
  }
}
