package com.ukb.IGSB.TsvVcfUtils.score_server;

import com.beust.jcommander.Parameter;

public class ScoreServerArgs {

  @Parameter(names = "--help", help = true)
  private boolean help = false;

  public boolean isHelp() {
    return help;
  }

  @Parameter(names = "--vcf-database", description = "Path to the vcf file to use as database")
  private String vcf_db;

  public String getVcfDB() {
    return vcf_db;
  }
}
