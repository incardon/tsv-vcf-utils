package com.ukb.IGSB.TsvVcfUtils.vcf_rewriter;

import com.beust.jcommander.Parameter;
import java.util.List;

public class VcfRewriteArgs {

  @Parameter(names = "--help", help = true)
  private boolean help = false;

  public boolean isHelp() {
    return help;
  }

  @Parameter(names = "--vcf-select-rules", description = "Add vcf rewrite rules")
  private List<String> selects;

  @Parameter(names = "--vcf-concat-rules", description = "Add vcf concatenation rules")
  private List<String> concats;

  @Parameter(names = "--vcf-rewrite-cols", description = "Specify which columns to rewrite")
  private List<Integer> rewrite;

  @Parameter(names = "--vcf-in", description = "Vcf to read")
  private String VcfIN;

  @Parameter(names = "--vcf-out", description = "Vcf to write")
  private String VcfOUT;

  public List<String> getVcfSelects() {
    return selects;
  }

  public List<String> getVcfConcats() {
    return concats;
  }

  public List<Integer> getVcfRewrite() {
    return rewrite;
  }

  public String getVcfIN() {
    return VcfIN;
  }

  public String getVcfOUT() {
    return VcfOUT;
  }
}
