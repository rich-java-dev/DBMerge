package core;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class App {

  private static Connection connect1 = null;
  private static Connection connect2 = null;

  private static Config config = Config.getInstance();

  public static void main(String[] args) {

    Logger.getInstance().log("log.txt", "DBMerge started");

    if (args.length > 0)
      config.put("db1.name", args[0]);
    if (args.length > 1)
      config.put("db2.name", args[1]);
    if (args.length > 2)
      config.put("module", args[2]);

    setConnections();
    process();
    cleanUp();
  }

  private static void setConnections() {
    try {
      boolean exportFiles = config.get("export", "false").equals("true") ? true : false;
      boolean importFiles = config.get("import", "false").equals("true") ? true : false;

      DBMS conn1DBMS = DBMS.valueOf(config.get("db1.dbms", "PSQL")); // BMS.PSQL;
      String conn1Srvr = config.get("db1.server", "localhost");
      String conn1Db = config.get("db1.database", "tempdb"); // "colonial";
      String conn1User = config.get("db1.user", "root");

      int port1 = Integer.valueOf(config.get("db1.port", "0"));
      if (port1 == 0)
        port1 = DBMS.getDefaultPort(conn1DBMS);

      DBMS conn2DBMS = DBMS.valueOf(config.get("db2.dbms", "PSQL")); // BMS.PSQL;
      String conn2Srvr = config.get("db2.server", "localhost");
      String conn2Db = config.get("db2.database", "tempdb"); // "colonial";
      String conn2User = config.get("db2.user", "root");

      int port2 = Integer.valueOf(config.get("db2.port", "0"));
      if (port2 == 0)
        port2 = DBMS.getDefaultPort(conn2DBMS);

      Scanner pwScanner = new Scanner(System.in);

      if (exportFiles) {
        System.out.println("Input source db pw:");
        String pw = pwScanner.nextLine();
        connect1 = getConnectionWithTimeout(conn1DBMS, conn1Srvr, conn1Db, port1, conn1User, pw, 8);
      }

      if (importFiles) {
        System.out.println("Input target db pw:");
        String pw = pwScanner.nextLine();
        connect2 = getConnectionWithTimeout(conn2DBMS, conn2Srvr, conn2Db, port2, conn2User, pw, 8);
      }

      pwScanner.close();

    } catch (Exception e) {
      Logger.getInstance().log("err.log", "%s", e);
    }
  }

  private static Connection getConnectionWithTimeout(DBMS dbms, String server, String database, int port, String user,
      String password, int timeout) throws InterruptedException, ExecutionException, TimeoutException {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Callable<Connection> cb = () -> {
      return DBMS.getConnection(dbms, server, database, port, user, password);
    };
    Future<Connection> timeoutConn = executor.submit(cb);
    Connection conn = timeoutConn.get(timeout, TimeUnit.SECONDS);

    executor.shutdown();

    return conn;
  }

  private static void process() {
    boolean exportFiles = config.get("export", "false").equals("true") ? true : false;
    boolean importFiles = config.get("import", "false").equals("true") ? true : false;
    String module = config.get("module", "mod1");

    String dir = config.get("dir", "");
    String delimiter = config.get("delimiter", "\t");
    DBMS dbms = DBMS.valueOf(config.get("db2.dbms", "MYSQL"));

    if (!dir.isEmpty()) {
      try {
        Files.createDirectories(new File(dir).toPath());
      } catch (Exception e) {
        Logger.getInstance().log("err.log", " Error while Merging DBs: %s ", e);
      }

    }

    for (Entry<Object, Object> entry : config.entrySet()) {
      try {

        String key = (String) entry.getKey();

        if (key.startsWith(module + ".")) {
          String val = (String) entry.getValue();
          String table = key.replaceFirst(module + ".", "");

          boolean clearTable = val.equals("O"); // Overwrite vs. Merge

          if (exportFiles)
            DBOperation.exportTable(table, connect1, dir, delimiter);
          if (importFiles)
            DBOperation.importTable(table, connect2, dbms, clearTable, dir, delimiter);

        }

      } catch (Exception e) {
        Logger.getInstance().log("err.log", " Error while Merging DBs: %s ", e);
        e.printStackTrace();
      }
    }

    if (exportFiles && !importFiles)
      zipData("xfer.zip", dir);

  }

  private static void zipData(String zipName, String dir) {
    try {
      Runtime.getRuntime().exec(String.format("7z a %s * %s", zipName, dir));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      Logger.getInstance().log("err.log", " Error while Zipping. Check if 7z is installed: %s ", e);
    }
  }

  private static void cleanUp() {
    try {
      for (Connection conn : Arrays.asList(connect1, connect2)) {
        if (conn != null && !conn.isClosed())
          conn.close();
      }

      Config.getInstance().deleteConfig();
      Config.getInstance().copyDefaultConfig();
      Logger.getInstance().close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
