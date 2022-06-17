package com.ukb.IGSB.TsvVcfUtils.init_db;

import static htsjdk.variant.variantcontext.writer.Options.INDEX_ON_THE_FLY;

import com.google.common.collect.ImmutableList;
import com.ukb.IGSB.TsvVcfUtils.TsvVcfUtilsException;
import com.ukb.IGSB.TsvVcfUtils.core.TSVColumnParser;
import com.ukb.IGSB.TsvVcfUtils.utils.UcscBinning;
import com.ukb.IGSB.TsvVcfUtils.utils.VariantDescription;
import com.ukb.IGSB.TsvVcfUtils.utils.VariantNormalizer;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.*;
import java.io.*;
import java.util.*;

public class TsvVcfFileImporter {

  class VariantRow {
    public String chromosome;

    public String old_chr;

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
  protected final String refFastaPath;

  /** Path to Public TSV files */
  private List<String> TsvFiles;

  /** Columns selected for each file * */
  private List<String> TsvColumns;

  /** Tsv selected columns in integer * */
  private List<TSVColumnParser> TsvColumnsInteger;

  /** Bed selected columns in integer * */
  private List<TSVColumnParser> BedColumnsInteger;

  /** Path to Public TSV files */
  private List<String> VcfFiles;

  /** Colums selected for each file * */
  private List<String> VcfColumns;

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

  private List<Integer> chr_start_tsv;

  private Boolean disableExtraAnno;

  /** From where start chromosome start stop reference alternative * */
  List<Integer> start_cols;

  /** columns that does not follow the standard end here. * */
  List<List<Boolean>> bugged_columns_tsv;

  /** The format of each TSV file * */
  List<List<Integer>> formatInt;

  /** Bed files * */
  List<String> BedFiles;

  /** Tsv fieldname * */
  List<String> TsvFieldname;

  /** Vcf fieldname * */
  List<String> VcfFieldname;

  /** Bed fieldname * */
  List<String> BedFieldname;

  /** VcfDB * */
  String VcfDB;

  /** Cols list * */
  String[] cols_list;

  /** MapM to MT * */
  Boolean mapMtoMT;

  boolean disableNormalization;

  int actual_allele = 1;

  int findId(String file, String col_name) throws IOException, TsvVcfUtilsException {

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

  /**
   * Construct the TsvVcfFileImporter object.
   *
   * @param TsvFiles Paths to TSV files
   */
  public TsvVcfFileImporter(
      String refFastaPath,
      String release,
      List<String> TsvFiles,
      List<String> VcfFiles,
      List<String> BedFiles,
      List<String> TsvColumns,
      List<String> VcfColumns,
      List<String> BedColumns,
      List<String> TsvFieldname,
      List<String> VcfFieldname,
      List<String> BedFieldname,
      List<String> format,
      String VcfDB,
      Boolean disableExtraAnno,
      Boolean disableNormalization,
      Boolean mapMtoMT)
      throws TsvVcfUtilsException, IOException {
    this.TsvFiles = TsvFiles;
    this.VcfFiles = VcfFiles;
    this.BedFiles = BedFiles;
    this.TsvColumns = TsvColumns;
    this.VcfColumns = VcfColumns;
    this.refFastaPath = refFastaPath;
    this.release = release;
    this.TsvFieldname = TsvFieldname;
    this.VcfFieldname = VcfFieldname;
    this.BedFieldname = BedFieldname;
    this.VcfDB = VcfDB;
    this.mapMtoMT = mapMtoMT;
    this.disableExtraAnno = disableExtraAnno;

    if (TsvFiles == null) {
      this.TsvFiles = new ArrayList<String>();
      this.TsvColumns = new ArrayList<String>();
      this.TsvFieldname = new ArrayList<String>();
    }

    if (VcfFiles == null) {
      this.VcfFiles = new ArrayList<String>();
      this.VcfColumns = new ArrayList<String>();
      this.VcfFieldname = new ArrayList<String>();
    }

    if (BedFiles == null) {
      this.BedFiles = new ArrayList<String>();
      this.BedFieldname = new ArrayList<String>();
    }

    this.disableNormalization = disableNormalization;

    lastParsedLineTsv = new ArrayList<VariantRow>(this.TsvFiles.size());
    lastParsedLineVcf = new ArrayList<VariantContext>(this.VcfFiles.size());
    lastParsedLineBed = new ArrayList<VariantRow>(this.BedFiles.size());

    this.start_cols = new ArrayList<Integer>(this.TsvFiles.size());
    for (Integer cl : this.start_cols) {
      cl = 0;
    }

    this.bugged_columns_tsv = new ArrayList<List<Boolean>>();

    for (int i = 0; i < this.TsvFiles.size(); i++) {
      this.bugged_columns_tsv.add(new ArrayList<Boolean>());

      for (int j = 0; j < 5; j++) {
        this.bugged_columns_tsv.get(i).add(Boolean.FALSE);
      }
    }

    if (TsvColumns.size() != TsvFiles.size()) {
      throw new TsvVcfUtilsException(
          "Error the you have "
              + TsvFiles.size()
              + " files, and I am expecting "
              + TsvFiles.size()
              + " columns selections, but i got "
              + TsvColumns.size(),
          new Exception(""));
    }

    TsvColumnsInteger = new ArrayList<>();
    for (int i = 0; i < TsvFiles.size(); i++) {
      TsvColumnsInteger.add(new TSVColumnParser());
      TsvColumnsInteger.get(i).parse(TsvColumns.get(i), new File(TsvFiles.get(i)));
    }

    lastParsedLineColumnsTsv = new ArrayList<>();

    for (int i = 0; i < this.TsvFiles.size(); i++) {
      lastParsedLineColumnsTsv.add(new String());
    }

    lastParsedLineColumnsVcf = new ArrayList<>();

    for (int i = 0; i < this.VcfFiles.size(); i++) {
      lastParsedLineColumnsVcf.add(new String());
    }

    lastParsedLineColumnsBed = new ArrayList<>();

    for (int i = 0; i < this.BedFiles.size(); i++) {
      lastParsedLineColumnsBed.add(new String());
    }

    List<String> valid_keywords = new ArrayList<String>();
    valid_keywords.add("chr");
    valid_keywords.add("start");
    valid_keywords.add("stop");
    valid_keywords.add("ref");
    valid_keywords.add("alt");

    if (format.size() != this.TsvFiles.size()) {
      throw new TsvVcfUtilsException(
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
          throw new TsvVcfUtilsException(
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
    for (int i = 0; i < this.BedFiles.size(); i++) {
      BedColumnsInteger.add(new TSVColumnParser());
      BedColumnsInteger.get(i).parse(BedColumns.get(i), new File(BedFiles.get(i)));
    }

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

    this.disableExtraAnno = disableExtraAnno;
  }

  /** Execute TSV file import. */
  public void run() throws Exception {
    System.err.println("Re-creating table in database...");
    //    recreateTable();

    System.err.println("Importing Tsv file ...");

    importBedTsvVcfFiles();

    System.err.println("Done with importing Tsv files...");
  }

  /** Has a variant * */
  private boolean hasVariantTsv(int i, List<BufferedReader> files, ChrIntHolder start) {
    if (lastParsedLineTsv.get(i) == null) {
      return false;
    }

    if (lastParsedLineTsv.get(i).start == start.svalue
        && getChrInteger(lastParsedLineTsv.get(i).chromosome) == start.cvalue) {
      return true;
    }

    return false;
  }

  /** Has a variant * */
  private boolean hasVariantVcf(
      int i, List<CloseableIterator<VariantContext>> files, ChrIntHolder start) {
    if (lastParsedLineVcf.get(i) == null) {
      return false;
    }

    if (lastParsedLineVcf.get(i).getStart() == start.svalue
        && getChrInteger(lastParsedLineVcf.get(i).getContig()) == start.cvalue) {
      return true;
    }

    return false;
  }

  /** Has a variant * */
  private VariantRow getNextVariantTsv(int i, List<BufferedReader> files, StringHolder cols)
      throws IOException, TsvVcfUtilsException {
    VariantRow toReturn = lastParsedLineTsv.get(i);
    cols.value = getTsvColumns(i);

    lastParsedLineTsv.set(i, parseTsv(i, files));

    return toReturn;
  }

  private void getBedAnno(int i, List<BufferedReader> files, String cols, VariantRowAnno vr)
      throws IOException, TsvVcfUtilsException {

    while (lastParsedLineBed.get(i) != null) {
      VariantRow bed = lastParsedLineBed.get(i);

      if (vr != null) {
        if (getChrInteger(vr.chromosome) < getChrInteger(lastParsedLineBed.get(i).chromosome)) {
          // No overlap rows with anno is bigger

          vr.anno_data += getBedNulls(i);

          break;
        } else {
          if (vr.start < bed.start) {
            if (vr.stop < bed.start) {
              // No overlap rows with anno is bigger

              vr.anno_data += getBedNulls(i);
              break;

            } else {
              // overlap

              vr.anno_data += getBedColumns(i);
              break;
            }
          } else {
            if (vr.start < bed.stop) {
              // overlap

              vr.anno_data += getBedColumns(i);
              break;
            } else {
              // no overlap, next interval
              lastParsedLineBed.set(i, parseBed(i, files));
            }
          }
        }
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

  /** ParseTsv * */
  VariantRow parseTsv(int i, List<BufferedReader> files) throws IOException, TsvVcfUtilsException {

    String line = files.get(i).readLine();
    if (line == null) {
      return null;
    }

    while (line.startsWith("#")) {
      line = files.get(i).readLine();
    }

    final String arr[] = line.split("\t");

    VariantRow vr = new VariantRow();
    vr.chromosome = arr[formatInt.get(i).get(0)];
    vr.start = Integer.parseInt(arr[formatInt.get(i).get(1)]);

    if (this.bugged_columns_tsv.get(i).get(2) == true) {
      vr.stop = vr.start;
    } else {
      try {
        if (formatInt.get(i).get(2) != -1) {
          vr.stop = Integer.parseInt(arr[formatInt.get(i).get(2)]);
        } else {
          vr.stop = vr.start;
        }
      } catch (NumberFormatException e) {
        this.bugged_columns_tsv.get(i).set(2, true);
        vr.stop = vr.start;
      }
    }

    vr.Reference = arr[formatInt.get(i).get(3)];

    vr.Alternative = arr[formatInt.get(i).get(4)];

    lastParsedLineColumnsTsv.set(i, new String());

    String cols = TsvColumnsInteger.get(i).getColumns(arr);

    lastParsedLineColumnsTsv.set(i, cols);

    vr.stop = vr.start + vr.Reference.length() - 1;

    return vr;
  }

  int chr_len[] = {
    249250621, 243199373, 198022430, 191154276, 180915260, 171115067, 159138663, 146364022,
    141213431, 135534747, 135006516, 133851895, 115169878, 107349540, 102531392, 90354753, 81195210,
    78077248, 59128983, 63025520, 48129895, 51304566, 155270560, 59373566, 16569
  };

  long tot_base;

  double percentage[];

  double progress(String chr, int start) {
    int chr_int = getChrInteger(chr) - 1;

    return percentage[chr_int]
        + (double) start / chr_len[chr_int] * (percentage[chr_int + 1] - percentage[chr_int]);
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

  VariantRow parseBed(int i, List<BufferedReader> files) throws IOException, TsvVcfUtilsException {

    String line = files.get(i).readLine();
    if (line == null) {
      return null;
    }

    while (line.startsWith("#")) {
      line = files.get(i).readLine();
    }

    final String[] arr = line.split("\t");

    VariantRow vr = new VariantRow();
    vr.chromosome = arr[0];
    vr.start = Integer.parseInt(arr[1]);
    vr.stop = Integer.parseInt(arr[2]);

    vr.Reference = "";
    vr.Alternative = "";

    lastParsedLineColumnsBed.set(i, new String());

    String cols = BedColumnsInteger.get(i).getColumns(arr);
    lastParsedLineColumnsBed.set(i, cols);

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
    return lastParsedLineColumnsBed.get(i).replace(";", " ");
  }

  class StringHolder {
    public String value;
  }

  class ChrIntHolder {
    public Integer svalue;
    public Integer cvalue;

    ChrIntHolder() {
      this.svalue = svalue;
      this.cvalue = cvalue;
    }

    @Override
    public String toString() {
      return String.valueOf(cvalue) + " " + String.valueOf(svalue);
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
    /*    String nulls = new String();

    for (int j = 0; j < TsvColumnsInteger.get(i).size(); j++) {
      nulls += "null ;";
    }*/

    return TsvColumnsInteger.get(i).getNulls();
  }

  String getVcfNulls(int i) {
    String nulls = new String();

    for (int j = 0; j < this.vcfColumnNumbers.get(i); j++) {
      nulls += "null ;";
    }

    return nulls;
  }

  String getBedNulls(int i) {

    return BedColumnsInteger.get(i).getNulls();
  }

  int debug_svalue = 0;
  int debug_cvalue = 0;

  /** Create rows * */
  private List<VariantRowAnno> create_rows(
      ChrIntHolder start,
      List<BufferedReader> tsvFiles,
      List<CloseableIterator<VariantContext>> VcfStreams,
      List<BufferedReader> BedStreams,
      VariantNormalizer normalizer)
      throws IOException, TsvVcfUtilsException {

    // rows to merge
    HashMap<VariantRow, List<Pair>> rows = new HashMap<VariantRow, List<Pair>>();

    StringHolder cols = new StringHolder();

    for (int i = 0; i < tsvFiles.size(); i++) {

      while (hasVariantTsv(i, tsvFiles, start)) {
        VariantRow vr = getNextVariantTsv(i, tsvFiles, cols);

        String alt_full = vr.Alternative;
        // Sometimes alternatives are listed with a comma
        for (String alt_s : alt_full.split(",")) {

          VariantRow vr_s = new VariantRow();

          if (this.disableNormalization) {

            vr_s.chromosome = vr.chromosome;
            vr_s.start = vr.start;
            vr_s.stop = vr.stop;
            vr_s.Reference = vr.Reference;
            vr_s.Alternative = alt_s;

          } else {
            VariantDescription vd =
                new VariantDescription(vr.chromosome, vr.start, vr.Reference, alt_s);
            VariantDescription vn = normalizer.normalizeInsertion(vd);

            if (vn.getPos() != start.svalue) {
              throw new TsvVcfUtilsException(
                  "Error, at position: "
                      + start.svalue
                      + " for file "
                      + TsvFiles.get(i)
                      + " variants are not normalized. You can can use this tool to create valid Vcf files from TSV files, normalize them with bcftools and finally merge them with this same tool");
            }

            vr_s.chromosome = vn.getChrom();
            vr_s.start = vn.getPos();
            vr_s.stop = vn.getEnd();
            vr_s.Reference = vn.getRef();
            vr_s.Alternative = vn.getAlt();
          }

          if (!rows.containsKey(vr_s)) {
            rows.put(vr_s, new ArrayList<Pair>());
          }

          List<Pair> r = rows.get(vr_s);
          r.add(new Pair(i, cols.value));
        }
      }
    }

    for (int i = 0; i < VcfStreams.size(); i++) {

      while (hasVariantVcf(i, VcfStreams, start)) {

        VariantRow vr = getNextVariantVcf(i, VcfStreams, cols);

        if (this.disableNormalization) {
          VariantDescription vd =
              new VariantDescription(vr.chromosome, vr.start, vr.Reference, vr.Alternative);
          VariantDescription vn = normalizer.normalizeInsertion(vd);

          vr.chromosome = vn.getChrom();
          vr.start = vn.getPos();
          vr.stop = vn.getEnd();
          vr.Reference = vn.getRef();
          vr.Alternative = vn.getAlt();
        }

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
    int chr_ = 100;

    // calculate next start
    for (VariantRow vr : lastParsedLineTsv) {

      if (vr != null) {
        int chr = getChrInteger(vr.chromosome);

        if (chr < chr_) {
          chr_ = chr;
          start_ = vr.start;
          continue;
        } else if (chr == chr_) {
          if (vr.start < start_) {
            chr_ = chr;
            start_ = vr.start;
          }
        }
      }
    }

    for (VariantContext vc : lastParsedLineVcf) {
      if (vc != null) { // && vc.getStart() < start_) {

        int chr = getChrInteger(vc.getContig());

        if (chr < chr_) {
          chr_ = chr;
          start_ = vc.getStart();
          continue;
        } else if (chr == chr_) {
          if (vc.getStart() < start_) {
            chr_ = chr;
            start_ = vc.getStart();
          }
        }
      }
    }

    start.svalue = start_;
    start.cvalue = chr_;

    if (start.cvalue < debug_cvalue) {

      String report = create_report();

      throw new TsvVcfUtilsException("Error files are not correctly sorted" + report);
    } else if (start.cvalue == debug_cvalue) {
      if (start.svalue < debug_svalue) {

        // Create report
        String report = create_report();

        throw new TsvVcfUtilsException("Error files are not correctly sorted\n" + report);
      }
    }

    debug_svalue = start.svalue;
    debug_cvalue = start.cvalue;

    for (int j = 0; j < rowsWithAnnoData.size(); j++) {

      for (int i = 0; i < BedStreams.size(); i++) {
        getBedAnno(i, BedStreams, cols.value, rowsWithAnnoData.get(j));
      }
    }

    return rowsWithAnnoData;
  }

  private String create_report() {
    String report = new String();

    report += "Previous chr = " + debug_cvalue + "\n";
    report += "Previous start = " + debug_svalue + "\n";

    int i = 0;
    for (VariantRow vr : lastParsedLineTsv) {
      if (vr != null) {
        report +=
            "File: "
                + TsvFiles.get(i)
                + "\n"
                + "      "
                + vr.chromosome
                + "\n"
                + "      "
                + vr.start
                + "\n";
      } else {
        report +=
            "File: " + TsvFiles.get(i) + "\n" + "      " + "null" + "\n" + "      " + "null" + "\n";
      }
      i++;
    }

    return report;
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

  private BufferedReader createStreamFromFilename(String pathFile) throws TsvVcfUtilsException {
    try {

      GZIPfileInput fileStream = new GZIPfileInput();
      BufferedReader buffered = fileStream.open(pathFile);

      return buffered;
    } catch (IOException e) {
      throw new TsvVcfUtilsException("Problem reading gziped TSV file", e);
    }
  }

  void create_header_file(
      List<String> TsvFieldName, List<String> VcfFieldName, List<String> BedFieldName, String name)
      throws IOException, TsvVcfUtilsException {

    BufferedWriter buffer = null;
    if (disableExtraAnno == false) {
      GZIPfileOutput file = new GZIPfileOutput();
      buffer = file.open("ExtraAnnosFields.tsv");
    }

    String cols = new String();

    int i = 0;
    for (String tsvfname : TsvFieldName) {

      if (TsvColumnsInteger.get(i).getNumberOfColumns() != tsvfname.split(":").length) {

        throw new TsvVcfUtilsException(
            "Error specified field names  "
                + tsvfname
                + " but it look like there are "
                + TsvColumnsInteger.get(i).getNumberOfColumns()
                + " columns selected\n",
            new Exception());
      }

      for (String col : tsvfname.split(":")) {
        cols += col + "\t";
      }
      i++;
    }

    i = 0;
    for (String vcffname : VcfFieldName) {

      if (VcfColumns.get(i).split(":").length != vcffname.split(":").length) {

        throw new TsvVcfUtilsException(
            "Error specified field names  "
                + vcffname
                + " but it look like there are "
                + VcfColumns.get(i).split(":").length
                + " columns selected\n",
            new Exception());
      }

      for (String col : vcffname.split(":")) {
        cols += col + "\t";
      }
      i++;
    }

    i = 0;
    for (String bedfname : BedFieldName) {

      if (BedColumnsInteger.get(i).getNumberOfColumns() != bedfname.split(":").length) {

        throw new TsvVcfUtilsException(
            "Error specified field names  "
                + bedfname
                + " but it look like there are "
                + BedColumnsInteger.get(i).getNumberOfColumns()
                + " columns selected\n",
            new Exception());
      }

      for (String col : bedfname.split(":")) {
        cols += col + "\t";
      }
      i++;
    }

    cols_list = cols.split("\t");

    if (buffer != null) {
      buffer.write(cols);
      buffer.close();
    }
  }

  public static int getChrInteger(String chr) {
    if (chr.equals("X")) {
      return 23;
    } else if (chr.equals("Y")) {
      return 24;
    } else if (chr.equals("MT") || chr.equals("M")) {
      return 25;
    }

    try {
      return Integer.parseInt(chr);
    } catch (Exception e) {
    }

    return -1;
  }

  /** Import the TSV database file */
  private void importBedTsvVcfFiles() throws Exception {

    VariantContextWriter writer = null;
    if (VcfDB != null) {
      System.out.println("Output: " + VcfDB);
      writer =
          new VariantContextWriterBuilder()
              .setOutputFile(VcfDB)
              .unsetOption(INDEX_ON_THE_FLY)
              .build();

      VCFHeader h = new VCFHeader();
      int i = 0;
      for (String files : this.TsvFieldname) {
        for (String col : files.split(":")) {
          VCFInfoHeaderLine line =
              new VCFInfoHeaderLine(
                  col,
                  VCFHeaderLineCount.A,
                  VCFHeaderLineType.String,
                  new File(this.TsvFiles.get(i)).getName() + " colum: " + col);
          h.addMetaDataLine(line);
        }
        i++;
      }

      i = 0;
      for (String files : this.VcfFieldname) {
        for (String col : files.split(":")) {
          VCFInfoHeaderLine line =
              new VCFInfoHeaderLine(
                  col,
                  VCFHeaderLineCount.A,
                  VCFHeaderLineType.String,
                  new File(this.VcfFiles.get(i)).getName() + " colum: " + col);
          h.addMetaDataLine(line);
        }
        i++;
      }

      i = 0;
      for (String files : this.BedFieldname) {
        for (String col : files.split(":")) {
          VCFInfoHeaderLine line =
              new VCFInfoHeaderLine(
                  col,
                  VCFHeaderLineCount.A,
                  VCFHeaderLineType.String,
                  new File(this.BedFiles.get(i)).getName() + " colum: " + col);
          h.addMetaDataLine(line);
        }
        i++;
      }
      // fill header

      writer.writeHeader(h);
    }

    BufferedWriter buffer = null;
    if (disableExtraAnno == false) {
      GZIPfileOutput file = new GZIPfileOutput();
      buffer = file.open("ExtraAnnos.tsv");
    }

    final VariantNormalizer normalizer = new VariantNormalizer(refFastaPath);

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

    ChrIntHolder start = new ChrIntHolder();
    start.svalue = 1000000000;
    start.cvalue = 100;

    EnumSet<VariantContext.Validation> validationToPerform =
        EnumSet.noneOf(VariantContext.Validation.class);
    validationToPerform.add(VariantContext.Validation.ALLELES);

    create_header_file(TsvFieldname, VcfFieldname, BedFieldname, "ExtraAnnosField.tsv");

    // Get the first line for each filegetAttributeAsString
    try {

      for (int i = 0; i < TsvStreams.size(); i++) {
        lastParsedLineTsv.add(parseTsv(i, TsvStreams));
        if (lastParsedLineTsv.get(i).start < start.svalue) {
          start.svalue = lastParsedLineTsv.get(i).start;
          start.cvalue = getChrInteger(lastParsedLineTsv.get(i).chromosome);
        }
      }

      for (int i = 0; i < VcfStreamsIt.size(); i++) {
        lastParsedLineVcf.add(VcfStreamsIt.get(i).next());
        if (lastParsedLineVcf.get(i).getStart() < start.svalue) {
          start.svalue = lastParsedLineVcf.get(i).getStart();
          start.cvalue = getChrInteger(lastParsedLineVcf.get(i).getContig());
        }
      }

      for (int i = 0; i < BedStreams.size(); i++) {
        lastParsedLineBed.add(parseBed(i, BedStreams));
      }

      // }

      VariantContextBuilder b = new VariantContextBuilder();

      double p_display = 0.0;
      int nit = 0;
      while (hasNextTsvOrVcf(TsvStreams, VcfStreamsIt)) {

        List<VariantRowAnno> annos_data =
            create_rows(start, TsvStreams, VcfStreamsIt, BedStreams, normalizer);

        VariantContext vc = null;
        List<Allele> alleles = new ArrayList<Allele>();

        for (VariantRowAnno va : annos_data) {

          String row = new String();
          row += "GRCh37\t";
          row += va.chromosome + "\t";
          row += va.start + "\t";
          row += va.start + va.Reference.length() - 1 + "\t";
          row += va.Reference + "\t";
          row += va.Alternative + "\t";
          row += va.bin + "\t";
          row += "[" + va.anno_data + "]" + "\n";

          if (va.anno_data.split(";").length != cols_list.length) {
            throw new TsvVcfUtilsException(
                "Error parsing colums, expected: "
                    + cols_list.length
                    + ", but got "
                    + va.anno_data.split(";").length
                    + " something is wrong with yours file at position "
                    + start.svalue
                    + " in chromosome "
                    + start.cvalue);
          }

          if (buffer != null) {
            buffer.write(row);
          }
        }

        double p = 0.0;
        if (nit % 100 == 0 && annos_data.size() != 0) {
          if (start.svalue < 500000000) {
            p = progress(annos_data.get(0).chromosome, start.svalue);
          } else {
            p = 100.0;
          }

          if ((p - p_display) >= 0.1) {
            p_display = p;
            System.out.println("Progress: " + Double.toString(p_display));
          }
        }

        if (VcfDB != null && annos_data.size() != 0) {

          // We have to separate alleles based on different references

          HashMap<String, List<VariantRowAnno>> hm = new HashMap<>();
          for (VariantRowAnno va : annos_data) {

            if (!hm.containsKey(va.Reference)) {
              hm.put(va.Reference, new ArrayList<VariantRowAnno>());
            }

            List<VariantRowAnno> vra = hm.get(va.Reference);
            vra.add(va);
          }

          Map<String, String> attr = new HashMap<String, String>();
          for (Map.Entry<String, List<VariantRowAnno>> entry : hm.entrySet()) {

            alleles.clear();
            int size = 0;
            for (VariantRowAnno v : entry.getValue()) {
              Allele al = null;
              if (v.Alternative.equals(".")) {
                al = Allele.create("*", false);
              } else {
                al = Allele.create(v.Alternative, false);
              }

              int i = 0;
              for (String prop : v.anno_data.split(";")) {
                if (!prop.equals("null ")) {

                  attr.put(cols_list[i], prop);
                }
                i++;
              }

              size = v.Reference.length();
              alleles.add(al);
            }

            alleles.add(Allele.create(entry.getKey(), true));

            if (annos_data.get(0).chromosome.equals("M") && this.mapMtoMT) {
              annos_data.get(0).chromosome = new String("MT");
            }

            VariantContext vcw =
                b.alleles(alleles)
                    .start((long) annos_data.get(0).start)
                    .stop((long) annos_data.get(0).start + size - 1)
                    .chr(annos_data.get(0).chromosome)
                    .attributes(attr)
                    .make();

            writer.add(vcw);
          }
        }

        nit++;
      }

      if (writer != null) {
        writer.close();
      }

      if (TsvStreams.size() != TsvFieldname.size()) {
        throw new TsvVcfUtilsException(
            "Error there are "
                + TsvStreams.size()
                + " TSV files (--tsv-files), but we specified "
                + TsvFieldname.size()
                + " fields name --tsv-fieldname\n",
            new Exception());
      }

      if (VcfStreamsIt.size() != VcfFieldname.size()) {
        throw new TsvVcfUtilsException(
            "Error there are "
                + VcfStreamsIt.size()
                + " Vcf files (--vcf-files), but we specified "
                + VcfFieldname.size()
                + "fields name --vcf-fieldname\n",
            new Exception());
      }

      if (BedStreams.size() != BedFieldname.size()) {
        throw new TsvVcfUtilsException(
            "Error there are "
                + BedStreams.size()
                + " Bed files (--bed-files), but we specified "
                + BedFieldname.size()
                + "fields name --bed-fieldname\n",
            new Exception());
      }

      if (buffer != null) {
        buffer.close();
      }

      // Write on file the table

    } catch (IOException e) {
      throw new TsvVcfUtilsException("Problem reading gziped TSV file", e);
    } catch (IllegalArgumentException e) {
      throw new TsvVcfUtilsException("SAM tools reported an error at position " + start.svalue, e);
    }
  }
}
