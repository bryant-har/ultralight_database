package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DBCatalog {
  private final Logger logger = LogManager.getLogger();
  private final HashMap<String, ArrayList<Column>> tables;
  private static DBCatalog db;
  private String dbDirectory;
  private Map<String, TableStats> tableStats;

  // Class to represent statistics for a table
  public static class TableStats {
    public final int tupleCount;
    public final Map<String, ColumnStats> columnStats;

    public TableStats(int tupleCount, Map<String, ColumnStats> columnStats) {
      this.tupleCount = tupleCount;
      this.columnStats = columnStats;
    }
  }

  public static class ColumnStats {
    public final int minValue;
    public final int maxValue;

    public ColumnStats(int minValue, int maxValue) {
      this.minValue = minValue;
      this.maxValue = maxValue;
    }
  }

  private DBCatalog() {
    this.tables = new HashMap<>();
    this.tableStats = new HashMap<>();
  }

  public static DBCatalog getInstance() {
    if (db == null) {
      db = new DBCatalog();
    }
    return db;
  }

  public void setDataDirectory(String directory) {
    try {
      dbDirectory = directory;

      // Read schema first
      BufferedReader br = new BufferedReader(new FileReader(directory + "/schema.txt"));
      String line;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split("\\s");
        String tableName = tokens[0];
        ArrayList<Column> cols = new ArrayList<>();
        for (int i = 1; i < tokens.length; i++) {
          cols.add(new Column(new Table(null, tableName), tokens[i]));
        }
        tables.put(tokens[0], cols);
      }
      br.close();

      // Create and load statistics
      createAndLoadStats();

    } catch (Exception e) {
      logger.error(e.getMessage());
      throw new RuntimeException("Failed to initialize database catalog", e);
    }
  }

  private void createAndLoadStats() throws IOException {
    StatsMaker statsMaker = new StatsMaker(dbDirectory);
    statsMaker.createStats();
    loadStats();
  }

  private void loadStats() throws IOException {
    String statsPath = dbDirectory + "/stats.txt";
    try (BufferedReader reader = new BufferedReader(new FileReader(statsPath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(" ");
        String tableName = parts[0];
        int tupleCount = Integer.parseInt(parts[1]);

        Map<String, ColumnStats> columnStats = new HashMap<>();

        // Process each column's statistics
        for (int i = 2; i < parts.length; i++) {
          String[] columnParts = parts[i].split(",");
          String columnName = columnParts[0];
          int min = Integer.parseInt(columnParts[1]);
          int max = Integer.parseInt(columnParts[2]);
          columnStats.put(columnName, new ColumnStats(min, max));
        }

        // Store the table statistics
        tableStats.put(tableName, new TableStats(tupleCount, columnStats));
      }
    }
  }

  public File getFileForTable(String tableName) {
    return new File(dbDirectory + "/data/" + tableName);
  }

  public ArrayList<Column> getColumns(String tableName) {
    return tables.get(tableName);
  }

  public HashMap<String, ArrayList<Column>> getTables() {
    return tables;
  }

  // Statistics accessor methods
  public TableStats getTableStats(String tableName) {
    return tableStats.get(tableName);
  }

  public int getTableTupleCount(String tableName) {
    TableStats stats = tableStats.get(tableName);
    return stats != null ? stats.tupleCount : 0;
  }

  public ColumnStats getColumnStats(String tableName, String columnName) {
    TableStats tableStats = this.tableStats.get(tableName);
    if (tableStats != null) {
      return tableStats.columnStats.get(columnName);
    }
    return null;
  }
}
