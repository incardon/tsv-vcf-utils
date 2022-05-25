package com.ukb.IGSB.TsvVcfUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import com.ukb.IGSB.TsvVcfUtils.init_db.InitDb;
import com.ukb.IGSB.TsvVcfUtils.init_db.InitDbArgs;
import com.ukb.IGSB.TsvVcfUtils.score_server.ScoreServer;
import com.ukb.IGSB.TsvVcfUtils.score_server.ScoreServerArgs;

public class VcfTsvUtilsCli {

  public static void main(String[] args) throws Exception {
    final InitDbArgs initDb = new InitDbArgs();
    final ScoreServerArgs scServer = new ScoreServerArgs();

    final JCommander jc =
        JCommander.newBuilder()
            .addCommand("init-db", initDb)
            .addCommand("score_server", scServer)
            .build();

    if ((args == null || args.length == 0)) {
      jc.usage();
      System.exit(1);
    }

    try {
      jc.parse(args);
    } catch (MissingCommandException e) {
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
        if (initDb.isHelp()) {
          jc.usage("score_server");
        } else {
          new ScoreServer(scServer.getVcfDB()).run();
        }
        break;
      default:
        System.err.println("Unknown command: " + cmd);
        System.exit(1);
    }
  }
}
