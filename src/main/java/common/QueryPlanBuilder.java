package common;

import java.util.ArrayList;
import java.util.List;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.OrderByElement;
import operator.*;

public class QueryPlanBuilder {
  public QueryPlanBuilder() {
  }

  /**
   * Top level method to translate statement to query plan
   *
   * @param stmt statement to be translated
   * @return the root of the query plan
   * @precondition stmt is a Select having a body that is a PlainSelect
   */
  public Operator buildPlan(Statement stmt) throws NotImplementedException {
    if (!(stmt instanceof Select)) {
      throw new IllegalArgumentException("Only SELECT statements are supported.");
    }
    Select select = (Select) stmt;
    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
    FromItem fromItem = plainSelect.getFromItem();
    Operator root;
    DBCatalog dbCatalog = DBCatalog.getInstance();

    if (fromItem instanceof Table) {
      String tableName = ((Table) fromItem).getName();
      ArrayList<Column> tableColumns = dbCatalog.getColumns(tableName);
      root = new ScanOperator(tableColumns, tableName);

      // Check for ORDER BY clause
      List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
      if (orderByElements != null && !orderByElements.isEmpty()) {
        root = new SortOperator(tableColumns, root, orderByElements);
      }

      return root;
    } else {
      throw new NotImplementedException("Only single table queries are supported.");
    }
  }
}