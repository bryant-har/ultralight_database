package common;

import file_management.TupleReader;
import java.io.*;
import java.util.*;
import net.sf.jsqlparser.schema.Column;

public class StatsMaker {
  private final String dbDirectory;
  private final DBCatalog catalog;

  public StatsMaker(String dbDirectory) {
    this.dbDirectory = dbDirectory;
    this.catalog = DBCatalog.getInstance();
  }

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

        // Stats for each column
        for (int i = 0; i < columns.size(); i++) {
          String columnName = columns.get(i).getColumnName();
          int min = Integer.MAX_VALUE;
          int max = Integer.MIN_VALUE;

          for (int[] tuple : tuples) {
            min = Math.min(min, tuple[i]);
            max = Math.max(max, tuple[i]);
          }

          line.append(" ")
              .append(columnName)
              .append(",")
              .append(min)
              .append(",")
              .append(max);
        }

        writer.write(line.toString());
        writer.newLine();
        reader.close();
      }
    }
  }
}