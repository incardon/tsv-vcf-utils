package com.ukb.IGSB.TsvVcfUtils.init_db;

import com.ukb.IGSB.TsvVcfUtils.TsvVcfUtilsException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/** Implementation of the <tt>init-db</tt> command. */
public final class InitDb {

  public static final int VARCHAR_LEN = 1000;

  /** Configuration for the command. */
  private final InitDbArgs args;

  /** Construct with the given configuration. */
  public InitDb(InitDbArgs args) {
    this.args = args;
  }

  /** Execute the command. */
  public void run() {
    System.err.println("Running init-db; args: " + args);

    try (Connection conn = null /*DriverManager.getConnection(
            "jdbc:h2:"
                + args.getDbPath()
                + ";TRACE_LEVEL_FILE=0;MV_STORE=FALSE;MVCC=FALSE"
                + ";DB_CLOSE_ON_EXIT=FALSE",
            "sa",
            "")*/) {
      if (args.getTsvPaths() != null && args.getTsvPaths().size() > 0) {
        try {
          new TsvVcfFileImporter(
                  args.getRefPath(),
                  args.getRelease(),
                  args.getTsvPaths(),
                  args.getVcfPaths(),
                  args.getBedPaths(),
                  args.getTsvColumns(),
                  args.getVcfColums(),
                  args.getBedColums(),
                  args.getTsvFieldnames(),
                  args.getVcfFieldnames(),
                  args.getBedFieldnames(),
                  args.getFormat(),
                  args.getVcfDB())
              .run();
        } catch (IOException e) {
          throw new TsvVcfUtilsException("Error, reading database files: ", e);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      if (args.getTestTsvPaths() != null && args.getTestTsvPaths().size() > 0) {
        try {
          new TestFiles(args.getTestTsvPaths(), args.getMargin(), args.getFormat()).run();
        } catch (IOException e) {
          throw new TsvVcfUtilsException("Error, reading database files: ", e);
        }
      }
      if (args.getTsvSortChrFiles() != null && args.getTsvSortChrFiles().size() != 0) {
        try {
          new ChromosomeSort(args.getTsvSortChrFiles(), args.getFormat()).run();
        } catch (IOException e) {
          throw new TsvVcfUtilsException("Error, reading database files: ", e);
        }
      }
    } catch (SQLException e) {
      System.err.println("Problem with database conection");
      e.printStackTrace();
      System.exit(1);
    } catch (TsvVcfUtilsException e) {
      System.err.println("Problem executing init-db");
      e.printStackTrace();
      System.exit(1);
    }
  }
}
