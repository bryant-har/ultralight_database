package operator;

import common.DBCatalog;
import common.Tuple;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;

/** Class to represent scan operators. e.g. select * from table */
public class ScanOperator extends Operator {
  private int cursor = 0;
  private ArrayList<String> tableData;

  public ScanOperator(ArrayList<Column> outputSchema, String tableName) {
    super(outputSchema);
    tableData = readTableFile(tableName);
  }

  /**
   * Reads a table file, deserializes each line, and returns the rows
   *
   * @param tableName
   */
  private ArrayList<String> readTableFile(String tableName) {
    DBCatalog dbDirectory = DBCatalog.getInstance();
    File tableFile = dbDirectory.getFileForTable(tableName);
    ArrayList<String> rows = new ArrayList<>();
    try (InputStream inputStream = new FileInputStream(tableFile);
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = br.readLine()) != null) {
        String serializedTokens = line.replaceAll("\\s+", ",");
        rows.add(serializedTokens);
      }
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return rows;
  }

  /** Resets cursor on the operator to the beginning */
  public void reset() {
    cursor = 0;
  };

  /**
   * Get next tuple from operator
   *
   * @return next Tuple, or null if we are at the end
   */
  public Tuple getNextTuple() {
    if (cursor < tableData.size()) {
      return new Tuple(tableData.get(cursor++));
    }
    return null;
  }

  /**
   * Collects all tuples of this operator.
   *
   * @return A list of Tuples.
   */
  public List<Tuple> getAllTuples() {
    Tuple t;
    List<Tuple> tuples = new ArrayList<>();
    while ((t = getNextTuple()) != null) {
      tuples.add(t);
    }

    return tuples;
  }

  /**
   * Iterate through output of operator and send it all to the specified
   * printStream)
   *
   * @param printStream stream to receive output, one tuple per line.
   */
  public void dump(PrintStream printStream) {
    Tuple t;
    while ((t = this.getNextTuple()) != null) {
      printStream.println(t);
    }
  }
}
