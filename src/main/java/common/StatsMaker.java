package common;

import file_management.TupleReader;
import java.io.*;
import java.util.*;
import net.sf.jsqlparser.schema.Column;

/**
 * The {@code StatsMaker} class is responsible for generating statistical metadata about the tables
 * in a database. This metadata includes the number of tuples in a table and the minimum and maximum
 * values for each column. The generated statistics are saved to a file named {@code stats.txt}.
 *
 * <h2>Key Features</h2>
 *
 * <ul>
 *   <li>Scans all tables in the database and calculates statistics for each.
 *   <li>Writes the statistics in a structured format to a file for easy access.
 *   <li>Useful for query optimization and database management.
 * </ul>
 *
 * <h2>Statistics Format</h2>
 *
 * Each line in the {@code stats.txt} file represents the statistics for a single table:
 *
 * <pre>
 * TableName TupleCount Column1Name,min,max Column2Name,min,max ...
 * </pre>
 *
 * Example:
 *
 * <pre>
 * Employees 1000 Age,18,65 Salary,30000,120000
 * </pre>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * StatsMaker statsMaker = new StatsMaker("/path/to/db");
 * statsMaker.createStats();
 * }</pre>
 *
 * @see DBCatalog
 * @see TupleReader
 */
public class StatsMaker {
  private final String dbDirectory; // Directory where the database files are stored
  private final DBCatalog catalog; // Database catalog for accessing table metadata

  /**
   * Constructs a {@code StatsMaker} for a given database directory.
   *
   * @param dbDirectory Path to the database directory.
   */
  public StatsMaker(String dbDirectory) {
    this.dbDirectory = dbDirectory;
    this.catalog = DBCatalog.getInstance();
  }

  /**
   * Generates statistics for all tables in the database and writes them to {@code stats.txt}.
   *
   * @throws IOException If an I/O error occurs during reading or writing.
   */
  public void createStats() throws IOException {
    String statsPath = dbDirectory + "/stats.txt";

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(statsPath))) {
      Map<String, ArrayList<Column>> tables = catalog.getTables();

      for (Map.Entry<String, ArrayList<Column>> entry : tables.entrySet()) {
        String tableName = entry.getKey();
        ArrayList<Column> columns = entry.getValue();

        // Get tuples for this table
        String filePath = dbDirectory + "/data/" + tableName;
        TupleReader reader = new TupleReader(filePath);
        List<int[]> tuples = reader.readTuples();

        // Format: TableName TupleCount Col1,min,max Col2,min,max ...
        StringBuilder line = new StringBuilder();
        line.append(tableName).append(" ");
        line.append(tuples.size());

        // Compute stats for each column
        for (int i = 0; i < columns.size(); i++) {
          String columnName = columns.get(i).getColumnName();
          int min = Integer.MAX_VALUE;
          int max = Integer.MIN_VALUE;

          for (int[] tuple : tuples) {
            min = Math.min(min, tuple[i]);
            max = Math.max(max, tuple[i]);
          }

          line.append(" ").append(columnName).append(",").append(min).append(",").append(max);
        }

        // Write stats for the current table
        writer.write(line.toString());
        writer.newLine();

        // Close the reader for the current table
        reader.close();
      }
    }
  }
}
