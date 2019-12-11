package core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public enum DBMS {
  MSSQL, MYSQL, AS400, ORACLE, NONE;

  private Connection connection = null;

  private String server = "";
  private String database = "";
  private int port;

  private static void loadClass(DBMS dbms) throws ClassNotFoundException {
    switch (dbms) {

    case MSSQL:
      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
      break;

    case MYSQL:
      Class.forName("com.mysql.cj.jdbc.Driver");
      break;

    case AS400:
      Class.forName("com.ibm.as400.access.AS400JDBCDriver");
      break;

    case ORACLE:
      Class.forName("oracle.jdbc.driver.OracleDriver");
      break;

    default:
      break;
    }
  }

  public static DBMS getDBMS(String name) {
    return asList().stream().filter(dbms -> dbms.toString().equals(name)).findFirst().get();
  }

  public static String getDbUrl(DBMS dbms, String server, String database, int port) {
    switch (dbms) {
    case MSSQL:
      return "jdbc:sqlserver://" + server + ":" + port + ";" + "databaseName=" + database
          + ";sendStringParametersAsUnicode=false;";

    case MYSQL:
      return "jdbc:mysql://" + server + ":" + port + "/" + database
          + "?useSSL=false&useServerPrepStmts=true&defaultFetchSize=1000&useCursorFetch=true";

    case AS400:
      return "jdbc:as400://" + server + ":" + database;

    case ORACLE:
      return "jdbc:oracle:thin:@" + server + ":" + port + "/" + database;

    default:
      break;

    }
    return "";
  }

  public static String getDbUrl(DBMS dbms, String server, String database) {
    return getDbUrl(dbms, server, database, getDefaultPort(dbms));
  }

  public static int getDefaultPort(DBMS dbms) {
    switch (dbms) {
    case MSSQL:
      return 1433;
    case MYSQL:
      return 3306;
    case AS400:
      return 446;
    case ORACLE:
      return 1521;
    default:
      return 0;
    }
  }

  public String getURL(String server, String database) {
    return getURL(server, database, getDefaultPort(this));
  }

  public String getURL() {
    switch (this) {
    case MSSQL:
      return "jdbc:sqlserver://" + server + ":" + port + ";" + "databaseName=" + database
          + ";sendStringParametersAsUnicode=false;";

    case MYSQL:
      return "jdbc:mysql://" + server + ":" + port + "/" + database
          + "?useSSL=false&useServerPrepStmts=true&defaultFetchSize=1000&useCursorFetch=true";

    case AS400:
      return "jdbc:as400://" + server + ":" + database;

    case ORACLE:
      return "jdbc:oracle:thin:@" + server + ":" + port + "/" + database;

    default:
      break;
    }
    return "";
  }

  public String getURL(String server, String database, int port) {
    switch (this) {
    case MSSQL:
      return "jdbc:sqlserver://" + server + ":" + port + ";" + "databaseName=" + database
          + ";sendStringParametersAsUnicode=false;";

    case MYSQL:
      return "jdbc:mysql://" + server + ":" + port + "/" + database
          + "?useSSL=false&useServerPrepStmts=true&defaultFetchSize=1000&useCursorFetch=true";

    case AS400:
      return "jdbc:as400://" + server + ":" + database;

    case ORACLE:
      return "jdbc:oracle:thin:@" + server + ":" + port + "/" + database;

    default:
      break;
    }

    return "";
  }

  public static Connection getConnection(DBMS dbms, String server, String database, int port, String user, String pass)
      throws SQLException, ClassNotFoundException {
    loadClass(dbms);
    return DriverManager.getConnection(getDbUrl(dbms, server, database, port), user, pass);
  }

  public static Connection getConnection(DBMS dbms, String server, String database, String user, String pass)
      throws SQLException, ClassNotFoundException {
    loadClass(dbms);
    return DriverManager.getConnection(getDbUrl(dbms, server, database), user, pass);
  }

  public static Connection getConnection(DBMS dbms, String url, String user, String pass)
      throws SQLException, ClassNotFoundException {
    loadClass(dbms);
    return DriverManager.getConnection(url, user, pass);
  }

  public static Connection getConnection(DBMS dbms, String url) throws SQLException, ClassNotFoundException {
    loadClass(dbms);
    return DriverManager.getConnection(url);
  }

  public String getUser() {
    switch (this) {
    default:
      return "root";
    }
  }

  /**
   * Requires sqljdbc_auth.dll file, which is included in the sql server jdbc
   * driver
   */
  public static Connection getConnectionWindowsAuth(String server, String database, int port)
      throws SQLException, ClassNotFoundException {
    loadClass(DBMS.MSSQL);
    return DriverManager.getConnection(getDbUrl(DBMS.MSSQL, server, database, port) + "integratedSecurity=true;");
  }

  /**
   * Requires sqljdbc_auth.dll file, which is included in the sql server jdbc
   * driver
   */
  public static Connection getConnectionWindowsAuth(String server, String database)
      throws SQLException, ClassNotFoundException {
    loadClass(DBMS.MSSQL);
    return DriverManager.getConnection(getDbUrl(DBMS.MSSQL, server, database) + "integratedSecurity=true;");
  }

  public void createConnection(String user, String pass) throws SQLException, ClassNotFoundException {
    loadClass(this);
    connection = DriverManager.getConnection(getURL(), user, pass);
  }

  public void createConnection(String server, String database, int port, String user, String pass)
      throws SQLException, ClassNotFoundException {
    loadClass(this);
    connection = DriverManager.getConnection(getURL(server, database, port), user, pass);
  }

  public void createConnection(String server, String database, String user, String pass)
      throws SQLException, ClassNotFoundException {
    loadClass(this);
    connection = DriverManager.getConnection(getURL(server, database), user, pass);
  }

  public Connection getConnection() {
    return connection;
  }

  public void setParams(String server, String database, int port) {
    this.server = server;
    this.database = database;
    this.port = port;
  }

  public void setParams(String server, String database) {
    this.server = server;
    this.database = database;
  }

  public void closeConnection() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  private static List<DBMS> asList() {
    return new ArrayList<DBMS>(Arrays.asList(DBMS.values()));
  }

}
