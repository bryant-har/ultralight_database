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

        String tablePath = dbDirectory + "/data/" + tableName;
        TupleReader reader = new TupleReader(tablePath);
        List<int[]> tuples = reader.readTuples();

        int tupleCount = tuples.size();
        StringBuilder statsLine = new StringBuilder();
        statsLine.append(tableName).append(" ").append(tupleCount);

        for (int i = 0; i < columns.size(); i++) {
          String columnName = columns.get(i).getColumnName();
          int min = Integer.MAX_VALUE;
          int max = Integer.MIN_VALUE;

          for (int[] tuple : tuples) {
            int value = tuple[i];
            min = Math.min(min, value);
            max = Math.max(max, value);
          }

          statsLine.append(" ").append(columnName)
              .append(",").append(min)
              .append(",").append(max);
        }

        writer.write(statsLine.toString());
        writer.newLine();
        reader.close();
      }
    }
  }
}
