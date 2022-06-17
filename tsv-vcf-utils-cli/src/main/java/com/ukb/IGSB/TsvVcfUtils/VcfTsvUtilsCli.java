package com.ukb.IGSB.TsvVcfUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import com.ukb.IGSB.TsvVcfUtils.init_db.InitDb;
import com.ukb.IGSB.TsvVcfUtils.init_db.InitDbArgs;
import com.ukb.IGSB.TsvVcfUtils.score_server.ScoreServer;
import com.ukb.IGSB.TsvVcfUtils.score_server.ScoreServerArgs;
import com.ukb.IGSB.TsvVcfUtils.vcf_rewriter.VcfRewriteArgs;
import com.ukb.IGSB.TsvVcfUtils.vcf_rewriter.VcfRewriter;

public class VcfTsvUtilsCli {

  public static void main(String[] args) throws Exception {
    final InitDbArgs initDb = new InitDbArgs();
    final ScoreServerArgs scServer = new ScoreServerArgs();
    final VcfRewriteArgs vcfRewrite = new VcfRewriteArgs();

    final JCommander jc =
        JCommander.newBuilder()
            .addCommand("init-db", initDb)
            .addCommand("score_server", scServer)
            .addCommand("vcf_rewriter", vcfRewrite)
            .build();

    if ((args == null || args.length == 0)) {
      jc.usage();
      System.exit(1);
    }

    try {
      jc.parse(args);
    } catch (MissingCommandException e) {
      System.err.println(e.toString());
      jc.usage();
      System.exit(1);
      return;
    }

    final String cmd = jc.getParsedCommand();
    if (cmd == null) {
      jc.usage();
      System.exit(1);
    }

    switch (cmd) {
      case "init-db":
        if (initDb.isHelp()) {
          jc.usage("init-db");
        } else {
          new InitDb(initDb).run();
        }
        break;
      case "score_server":
        if (scServer.isHelp()) {
          jc.usage("score_server");
        } else {
          new ScoreServer(scServer.getVcfDB()).run();
        }
        break;
      case "vcf_rewriter":
        if (vcfRewrite.isHelp()) {
          jc.usage("vcf_rewrite");
        } else {
          new VcfRewriter(
                  vcfRewrite.getVcfIN(),
                  vcfRewrite.getVcfOUT(),
                  vcfRewrite.getVcfSelects(),
                  vcfRewrite.getVcfConcats(),
                  vcfRewrite.getVcfRewrite())
              .run();
        }
        break;
      default:
        System.err.println("Unknown command: " + cmd);
        System.exit(1);
    }
  }
}
