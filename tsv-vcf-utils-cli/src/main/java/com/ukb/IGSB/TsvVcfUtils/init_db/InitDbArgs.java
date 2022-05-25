package com.ukb.IGSB.TsvVcfUtils.init_db;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.List;

/**
 * JCommander command for <tt>varfish_annotator init-db</tt>.
 *
 * @author <a href="mailto:manuel.holtgrewe@bihealth.de">Manuel Holtgrewe</a>
 */
@Parameters(commandDescription = "Initialize or update DB")
public final class InitDbArgs {

  @Parameter(names = "--help", help = true)
  private boolean help = false;

  @Parameter(
      names = "--db-path",
      description = "Path to H2 file to initialize/update",
      required = true)
  private String dbPath;

  @Parameter(
      names = "--ref-path",
      description = "Path to reference FASTA file, used for variant normalization",
      required = false)
  private String refPath;

  @Parameter(
      names = "--exac-path",
      description =
          "Path to ExAC VCF file to use for import, see documentation for more information")
  private String exacPath;

  @Parameter(
      names = "--gnomad-exomes-path",
      description =
          "Path to gnomAD exomes VCF file to use for import, see documentation for more information")
  private List<String> gnomadExomesPaths;

  @Parameter(names = "--create-vcf-db", description = "Create a vcf database")
  private String create_vcf_db;

  @Parameter(
      names = "--gnomad-genomes-path",
      description =
          "Path to gnomAD genomes VCF file to use for import, see documentation for more information")
  private List<String> gnomadGenomesPaths;

  @Parameter(
      names = "--thousand-genomes-path",
      description =
          "Path to 1000 genomes VCF file to use for import, see documentation for more information")
  private List<String> thousandGenomesPaths;

  @Parameter(
      names = "--clinvar-path",
      description =
          "Path to Clinvar TSV file(s) to use for import, see documentation for more information")
  private List<String> clinvarPaths;

  @Parameter(
      names = "--hgmd-public",
      description =
          "Path to HTMD Public TSV file to use for import, see documentation for more information")
  private String hgmdPublicPath;

  @Parameter(names = "--region", description = "Genomic region CHR:START-END (1-based) to import")
  private String genomicRegion;

  @Parameter(
      names = "--db-release-info",
      description = "Provide database release information as \"$db:$release\" for storage in DB")
  private List<String> dbReleaseInfos;

  @Parameter(names = "--release", description = "The genome release used", required = true)
  private String release;

  @Parameter(names = "--tsv-files", description = "A set of tsv file tables", required = false)
  private List<String> tsvFiles;

  @Parameter(
      names = "--tsv-test-files",
      description = "A set of tsv file tables to convert into tests",
      required = false)
  private List<String> tsvTestFiles;

  @Parameter(
      names = "--tsv-sort-chr-files",
      description = "A set of tsv file to sort by chromosome",
      required = false)
  private List<String> tsvSortChrFiles;

  @Parameter(names = "--format", description = "format of the tsv tables", required = false)
  private List<String> formatTsv;

  @Parameter(
      names = "--margin",
      description = "margin for the test file creation",
      required = false)
  private String margin;

  @Parameter(
      names = "--tsv-fieldname",
      description = "Name of the fields for each selected columns",
      required = false)
  private List<String> tsvFieldNames;

  @Parameter(
      names = "--vcf-fieldname",
      description = "Name of the fields for each selected columns",
      required = false)
  private List<String> vcfFieldNames;

  @Parameter(
      names = "--bed-fieldname",
      description = "Name of the fields for each selected columns",
      required = false)
  private List<String> bedFieldNames;

  @Parameter(names = "--vcf-files", description = "A set of vcf file tables", required = false)
  private List<String> vcfFiles;

  @Parameter(names = "--bed-files", description = "A set of bed file tables", required = false)
  private List<String> bedFiles;

  @Parameter(
      names = "--columns-vcf",
      description = "Selected columns for each vcf file",
      required = false)
  private List<String> vcfColumnsName;

  @Parameter(
      names = "--columns-tsv",
      description = "Selected columns for each tsv file",
      required = false)
  private List<String> tsvColumnsNames;

  @Parameter(
      names = "--columns-bed",
      description = "Selected columns for each bed file",
      required = false)
  private List<String> bedColumnsName;

  @Parameter(names = "--table-name", description = "Name of the table to create", required = false)
  private String tableName;

  public boolean isHelp() {
    return help;
  }

  public String getRefPath() {
    return refPath;
  }

  public String getDbPath() {
    return dbPath;
  }

  public String getExacPath() {
    return exacPath;
  }

  public List<String> getGnomadExomesPaths() {
    return gnomadExomesPaths;
  }

  public List<String> getGnomadGenomesPaths() {
    return gnomadGenomesPaths;
  }

  public List<String> getThousandGenomesPaths() {
    return thousandGenomesPaths;
  }

  public String getMargin() {
    return margin;
  }

  public String getVcfDB() {
    return create_vcf_db;
  };

  public List<String> getClinvarPaths() {
    return clinvarPaths;
  }

  public String getHgmdPublicPath() {
    return hgmdPublicPath;
  }

  public String getGenomicRegion() {
    return genomicRegion;
  }

  public List<String> getDbReleaseInfos() {
    return dbReleaseInfos;
  }

  public String getRelease() {
    return release;
  }

  public void setRelease(String release) {
    this.release = release;
  }

  public List<String> getTsvPaths() {
    return this.tsvFiles;
  }

  public List<String> getTestTsvPaths() {
    return this.tsvTestFiles;
  }

  public List<String> getTsvSortChrFiles() {
    return this.tsvSortChrFiles;
  }

  public List<String> getVcfPaths() {
    return this.vcfFiles;
  }

  public List<String> getBedPaths() {
    return this.bedFiles;
  }

  public List<String> getTsvColumns() {
    return this.tsvColumnsNames;
  }

  public List<String> getVcfColums() {
    return this.vcfColumnsName;
  }

  public List<String> getBedColums() {
    return this.bedColumnsName;
  }

  public List<String> getTsvFieldnames() {
    return this.tsvFieldNames;
  }

  public List<String> getVcfFieldnames() {
    return this.vcfFieldNames;
  }

  public List<String> getBedFieldnames() {
    return this.bedFieldNames;
  }

  public List<String> getFormat() {
    return formatTsv;
  }

  public String getTableName() {
    return this.tableName;
  }

  @Override
  public String toString() {
    return "InitDbArgs{"
        + "help="
        + help
        + ", dbPath='"
        + dbPath
        + '\''
        + ", refPath='"
        + refPath
        + '\''
        + ", exacPath='"
        + exacPath
        + '\''
        + ", gnomadExomesPaths='"
        + gnomadExomesPaths
        + '\''
        + ", gnomadGenomesPaths='"
        + gnomadGenomesPaths
        + '\''
        + ", thousandGenomesPaths="
        + thousandGenomesPaths
        + ", clinvarPaths="
        + clinvarPaths
        + ", hgmdPublicPath='"
        + hgmdPublicPath
        + '\''
        + ", genomicRegion='"
        + genomicRegion
        + '\''
        + ", dbReleaseInfos="
        + dbReleaseInfos
        + "'}";
  }
}
