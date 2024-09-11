package common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;

/**
 * Class to represent expression contexts.
 *
 * <p>Add more constructors as necessary
 */
public class ExpressionContext {
  private Map<String, Integer> columnIndices = new HashMap<String, Integer>();

  public ExpressionContext(List<Column> columns) {
    for (int i = 0; i < columns.size(); i++) {
      Column column = columns.get(i);
      columnIndices.put(column.getColumnName(), i);
    }
  }

  public int getColumnIndex(String columnName) {
    return columnIndices.get(columnName);
  }
}
