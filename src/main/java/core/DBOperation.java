package core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DBOperation {

  public static void exportTable(String table, Connection connect, String dir, String delimiter)
      throws SQLException, IOException {
    String filePath = dir + (dir.isEmpty() ? "" : "\\") + table + ".asc";
    Logger.getInstance().log("log.txt", "Exporting data from table '%s' to '%s'", table, filePath);

    try (BufferedWriter out = new BufferedWriter(new FileWriter(filePath, false))) {

      Statement stmt = connect.createStatement();
      ResultSet rs = stmt.executeQuery("select * from " + table);
      ResultSetMetaData meta = rs.getMetaData();

      StringBuilder builder = new StringBuilder();
      while (rs.next()) {
        builder.setLength(0);

        for (int i = 1; i <= meta.getColumnCount(); i++) {
          rs.getObject(i);

          if (meta.getColumnName(i).equals("Random_Seq"))
            builder.append(rs.getBigDecimal(i).toPlainString());

          else if (meta.getColumnType(i) != Types.DECIMAL && meta.getColumnType(i) != Types.INTEGER) {

            if ((String.valueOf(rs.getObject(i)).indexOf("\n") > -1
                || String.valueOf(rs.getObject(i)).indexOf("\r") > -1))
              builder.append(
                  String.valueOf(rs.getObject(i)).replace("\n", "\u00A4").replace("\r", "\u00A4").replace("|", "-"));
            else if (meta.getColumnTypeName(i).indexOf("VARCHAR") > -1
                && (rs.getObject(i) == null || rs.getObject(i).toString().length() == 0))
              builder.append(" ");
            else
              builder.append(String.valueOf(rs.getObject(i)).replace("|", "-"));

          } else
            builder.append(rs.getObject(i));

          builder.append(delimiter);
        }

        out.write(builder.toString() + "\r\n");

      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static synchronized void importTable(String table, Connection connect, DBMS dbms, boolean clearTable,
      String dir, String delimiter) throws SQLException, IOException {

    String filePath = dir + (dir.isEmpty() ? "" : "\\") + table + ".asc";

    Logger.getInstance().log("log.txt", "Importing data for table '%s' from '%s'", table, filePath);

    if (clearTable) {
      Logger.getInstance().log("log.txt", String.format("Clearing table '%s'", table));
      connect.createStatement().executeUpdate("delete from " + table);
    }

    int inserts = 0;
    switch (dbms) {
    case MSSQL:
      inserts = importMSSQLTable(table, connect, filePath, delimiter);
      break;
    case MYSQL:
      inserts = importMYSQLTable(table, connect, filePath.replace("\\", "\\\\"), delimiter);
      break;
    default:
      inserts = importTable(table, connect, filePath, delimiter);
      break;
    }

    Logger.getInstance().log("log.txt", String.format("'%s' - %d records inserted", table, inserts));

  }

  private static synchronized int importTable(String table, Connection connect, String filePath, String delimiter)
      throws IOException, SQLException {
    ResultSetMetaData meta = connect.createStatement().executeQuery("select * from " + table).getMetaData();

    List<String> columns = new ArrayList<>();
    for (int i = 1; i <= meta.getColumnCount(); i++)
      columns.add(meta.getColumnName(i));
    String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", table,
        columns.stream().collect(Collectors.joining(", ")),
        columns.stream().map(c -> "?").collect(Collectors.joining(", ")));

    PreparedStatement stmt = connect.prepareStatement(sql);

    BufferedReader reader = new BufferedReader(new FileReader(filePath));
    String line = "";
    int inserts = 0;
    while ((line = reader.readLine()) != null) {

      String[] args = line.split(delimiter);

      int k = 0;
      for (String param : args) {
        stmt.setObject(++k, param);
      }

      try {
        inserts += stmt.executeUpdate();
      } catch (Exception e) {
        Logger.getInstance().log("err.log", "%s: %s", e, table);
      }

    }
    reader.close();

    return inserts;
  }

  private static synchronized int importMSSQLTable(String table, Connection connect, String filePath, String delimiter)
      throws SQLException {
    return connect.createStatement().executeUpdate(
        String.format("BULK INSERT %s FROM '%s' WITH (FIELDTERMINATOR='%s')", table, filePath, delimiter));
  }

  private static int importMYSQLTable(String table, Connection connect, String file, String delimiter)
      throws SQLException {
    return connect.createStatement().executeUpdate(
        String.format("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '%s'", file, table, delimiter));
  }

  /**
   * Copies contents from the "from" database table into the "to" database table,
   * 
   */
  @Deprecated
  public static void copy(String table, Connection from, Connection to, boolean clearTable) throws SQLException {

    if (clearTable) {
      to.createStatement().executeUpdate("DELETE FROM " + table);
    }

    try (PreparedStatement s1 = from.prepareStatement("select * from " + table); ResultSet rs = s1.executeQuery()) {
      ResultSetMetaData meta = rs.getMetaData();

      List<String> columns = new ArrayList<>();
      for (int i = 1; i <= meta.getColumnCount(); i++)
        columns.add(meta.getColumnName(i));

    }
  }

}
