package common;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import operator.logical.*;
import operator.physical.*;

public class PhysicalPlanBuilder implements LogicalOperatorVisitor {
  private Operator result;
  private Map<String, String> tableAliases;
  private boolean useIndexes;
  private DBCatalog dbCatalog;
  private String dbDirectory;

  public PhysicalPlanBuilder(Map<String, String> tableAliases, boolean useIndexes, String dbDirectory) {
    this.tableAliases = tableAliases;
    this.useIndexes = useIndexes;
    this.dbCatalog = DBCatalog.getInstance();
    this.dbDirectory = dbDirectory;
  }

  public Operator getResult() {
    return result;
  }

  @Override
  public void visit(LogicalScanOperator op) {
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    result = new ScanOperator(schema, op.getTable().getName());
  }

  @Override
  public void visit(LogicalSelectOperator op) {
    // First check if we can use an index
    LogicalOperator child = op.getChildren().get(0);

    // Only try to use index if:
    // 1. Indexes are enabled in config
    // 2. Child is a scan operator (selection is on base table)
    if (useIndexes && child instanceof LogicalScanOperator) {
      LogicalScanOperator scanOp = (LogicalScanOperator) child;
      Table table = scanOp.getTable();
      String tableName = table.getName();

      // Check if there's an index available for this table
      String indexFile = getIndexFileForTable(tableName);
      if (indexFile != null) {
        String indexedColumn = getIndexedColumnForTable(tableName);
        boolean isClustered = isIndexClustered(tableName);

        // Analyze the selection condition
        SelectionAnalyzer analyzer = new SelectionAnalyzer(tableName, indexedColumn);
        op.getCondition().accept(analyzer);

        if (analyzer.hasIndexConditions()) {
          // Create an IndexScanOperator with ArrayList<Column>
          ArrayList<Column> scanSchema = new ArrayList<>(scanOp.getSchema());
          result = new IndexScanOperator(
              scanSchema,
              tableName,
              indexFile,
              isClustered,
              analyzer.getLowKey(),
              analyzer.getHighKey());

          // If there are remaining conditions, add a SelectOperator on top
          List<Expression> remainingConditions = analyzer.getRemainingConditions();
          if (!remainingConditions.isEmpty()) {
            // Combine remaining conditions with AND
            Expression remainingExpr = remainingConditions.get(0);
            for (int i = 1; i < remainingConditions.size(); i++) {
              remainingExpr = new AndExpression(remainingExpr, remainingConditions.get(i));
            }
            result = new SelectOperator(result, remainingExpr, tableAliases);
          }
          return;
        }
      }
    }

    // If we get here, either we can't use an index or chosen not to
    // Fall back to regular selection
    child.accept(this);
    Operator childOp = result;
    result = new SelectOperator(childOp, op.getCondition(), tableAliases);
  }

  private String getIndexFileForTable(String tableName) {
    // Looking in db/indexes directory with pattern tablename.columnname
    File indexesDir = new File(dbDirectory + "/indexes");
    if (indexesDir.exists() && indexesDir.isDirectory()) {
      File[] files = indexesDir.listFiles((dir, name) -> name.startsWith(tableName + "."));
      if (files != null && files.length > 0) {
        return files[0].getAbsolutePath();
      }
    }
    return null;
  }

  private String getIndexedColumnForTable(String tableName) {
    File indexInfoFile = new File(dbDirectory + "/index_info.txt");
    try (BufferedReader reader = new BufferedReader(new FileReader(indexInfoFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split("\\s+");
        if (parts[0].equals(tableName)) {
          return parts[1];
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  private boolean isIndexClustered(String tableName) {
    File indexInfoFile = new File(dbDirectory + "/index_info.txt");
    try (BufferedReader reader = new BufferedReader(new FileReader(indexInfoFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split("\\s+");
        if (parts[0].equals(tableName)) {
          return parts[2].equals("1");
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public void visit(LogicalProjectOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    result = new ProjectOperator(child, op.getSelectItems());
  }

  @Override
  public void visit(LogicalJoinOperator op) {
    op.getChildren().get(0).accept(this);
    Operator leftChild = result;
    op.getChildren().get(1).accept(this);
    Operator rightChild = result;
    result = new JoinOperator(leftChild, rightChild, op.getCondition(), tableAliases);
  }

  @Override
  public void visit(LogicalSortOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    result = new SortOperator(schema, child, op.getOrderByElements());
  }

  @Override
  public void visit(LogicalDuplicateEliminationOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    result = new DuplicateElementEliminationOperator(schema, child);
  }
}