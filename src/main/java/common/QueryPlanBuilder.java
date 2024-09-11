package common;

import java.util.ArrayList;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import operator.*;

/**
 * Class to translate a JSQLParser statement into a relational algebra query plan. For now only
 * works for Statements that are Selects, and specifically PlainSelects. Could implement the visitor
 * pattern on the statement, but doesn't for simplicity as we do not handle nesting or other complex
 * query features.
 *
 * <p>Query plan fixes join order to the order found in the from clause and uses a left deep tree
 * join. Maximally pushes selections on individual relations and evaluates join conditions as early
 * as possible in the join tree. Projections (if any) are not pushed and evaluated in a single
 * projection operator after the last join. Finally, sorting and duplicate elimination are added if
 * needed.
 *
 * <p>For the subset of SQL which is supported as well as assumptions on semantics, see the Project
 * 2 student instructions, Section 2.1
 */
public class QueryPlanBuilder {
  public QueryPlanBuilder() {}

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
      root = new SelectOperator(tableColumns, tableName, plainSelect.getWhere());
      return root;
    } else {
      throw new NotImplementedException("Only single table from items are supported.");
    }
  }
}
