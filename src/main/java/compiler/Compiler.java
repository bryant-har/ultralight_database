package compiler;

import common.DBCatalog;
import common.LogicalPlanBuilder;
import common.PhysicalPlanBuilder;
import common.StatsMaker;
import file_management.TupleWriter;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.Select;
import operator.logical.*;
import operator.physical.DuplicateElementEliminationOperator;
import operator.physical.ExternalSort;
import operator.physical.Operator;
import operator.physical.ProjectOperator;
import operator.physical.ScanOperator;
import operator.physical.SelectOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Compiler {
  private static final Logger logger = LogManager.getLogger(Compiler.class);
  private static String inputDir;
  private static String outputDir;
  private static String tempDir;

  public static void main(String[] args) {
    // Validate command line arguments
    if (args.length != 1) {
      logger.error("Usage: java -jar program.jar config_file");
      System.exit(1);
    }

    try {
      // Read directories from config file
      readConfig(args[0]);

      // Initialize database directory
      String dbDir = inputDir + File.separator + "db";

      // Initialize catalog and gather statistics
      DBCatalog.getInstance().setDataDirectory(dbDir);
      StatsMaker statsMaker = new StatsMaker(dbDir);
      statsMaker.createStats();

      // Read queries from file
      String queriesPath = inputDir + File.separator + "queries.sql";
      String queriesContent = new String(Files.readAllBytes(Paths.get(queriesPath)));
      Statements statements = CCJSqlParserUtil.parseStatements(queriesContent);

      // Create builders
      LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder();
      String indexDir = dbDir + File.separator + "indexes";
      PhysicalPlanBuilder physicalPlanBuilder =
          new PhysicalPlanBuilder(logicalPlanBuilder.getTableAliases(), indexDir);

      // Process each query
      int queryCount = 1;
      for (Statement statement : statements.getStatements()) {
        if (statement instanceof Select) {
          processQuery((Select) statement, queryCount++, logicalPlanBuilder, physicalPlanBuilder);
        }
      }

    } catch (Exception e) {
      logger.error("Error during compilation: ", e);
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void readConfig(String configPath) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(configPath))) {
      inputDir = reader.readLine();
      outputDir = reader.readLine();
      tempDir = reader.readLine();
    }
  }

  private static void processQuery(
      Select select,
      int queryNumber,
      LogicalPlanBuilder logicalPlanBuilder,
      PhysicalPlanBuilder physicalPlanBuilder)
      throws IOException {

    // Generate logical plan
    LogicalOperator logicalPlan = logicalPlanBuilder.buildPlan(select);

    // Generate physical plan
    logicalPlan.accept(physicalPlanBuilder);
    Operator physicalPlan = physicalPlanBuilder.getResult();

    // Write query plans
    writeQueryPlan(queryNumber, "logicalplan", LogicalOperatorFormatter.format(logicalPlan));
    writeQueryPlan(queryNumber, "physicalplan", OperatorFormatter.format(physicalPlan));

    // Execute query and write results
    String outputFile = outputDir + File.separator + "query" + queryNumber;
    TupleWriter writer = null;
    try {
      writer = new TupleWriter(outputFile);
      physicalPlan.dump(writer);
    } catch (Exception e) {
      logger.error("Error writing output for query {}: {}", queryNumber, e.getMessage());
      throw new IOException(e);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          logger.error("Error closing writer for query {}: {}", queryNumber, e.getMessage());
        }
      }
    }
  }

  private static void writeQueryPlan(int queryNum, String planType, String content)
      throws IOException {
    String filename = outputDir + File.separator + "query" + queryNum + "_" + planType;
    Files.write(Paths.get(filename), content.getBytes());
  }

  /** This class formats the physical plan into a tree-like string. */
  private static class OperatorFormatter {
    private static final String INDENT = "-";

    public static String format(Operator op) {
      if (op == null) return "";

      StringBuilder sb = new StringBuilder();

      if (op instanceof DuplicateElementEliminationOperator) {
        sb.append("DupElim\n");
        appendChildWithIndent(sb, op.getChild(), 1);
      } else if (op instanceof ExternalSort) {
        ExternalSort sort = (ExternalSort) op;
        String columns =
            sort.getOrderByElements().stream()
                .map(e -> ((Column) e.getExpression()).getFullyQualifiedName())
                .collect(Collectors.joining(", "));
        sb.append("ExternalSort[").append(columns).append("]\n");
        appendChildWithIndent(sb, op.getChild(), 2);
      } else if (op instanceof ProjectOperator) {
        ProjectOperator proj = (ProjectOperator) op;
        String columns =
            proj.getOutputSchema().stream()
                .map(Column::getFullyQualifiedName)
                .collect(Collectors.joining(", "));
        sb.append("Project[").append(columns).append("]\n");
        appendChildWithIndent(sb, op.getChild(), 3);
      } else if (op instanceof join_algorithms.BNLJ) {
        join_algorithms.BNLJ bnlj = (join_algorithms.BNLJ) op;
        sb.append("BNLJ[").append(bnlj.getJoinCondition()).append("]\n");
        appendChildWithIndent(sb, op.getChild(), 4);
      } else if (op instanceof join_algorithms.SMJ) {
        join_algorithms.SMJ smj = (join_algorithms.SMJ) op;
        sb.append("SMJ[").append(smj.getJoinCondition()).append("]\n");
        appendChildWithIndent(sb, op.getChild(), 4);
      } else if (op instanceof SelectOperator) {
        SelectOperator sel = (SelectOperator) op;
        sb.append("Select[").append(sel.getCondition()).append("]\n");
        appendChildWithIndent(sb, op.getChild(), 7);
      } else if (op instanceof ScanOperator) {
        ScanOperator scan = (ScanOperator) op;
        String tableName = scan.getOutputSchema().get(0).getTable().getName();
        sb.append("TableScan[").append(tableName).append("]");
      }

      return sb.toString();
    }

    private static void appendChildWithIndent(StringBuilder sb, Operator child, int numDashes) {
      if (child != null) {
        sb.append(" ");
        for (int i = 0; i < numDashes; i++) {
          sb.append(INDENT);
        }
        sb.append(format(child));
      }
    }
  }

  /**
   * This class formats the logical plan into the desired hierarchical format.
   *
   * <p>Expected format example: DupElim -Sort[S.A] --Project[S.A, R.G] ---Join[R.H <> B.D] [[S.B,
   * R.G], equals null, min null, max null] [[S.A, B.D], equals null, min null, max null] [[R.H],
   * equals null, min null, max 99] ----Leaf[Sailors] ----Select[R.H <= 99] -----Leaf[Reserves]
   * ----Leaf[Boats]
   */
  private static class LogicalOperatorFormatter {
    private static final String INDENT = "-";

    public static String format(LogicalOperator root) {
      StringBuilder sb = new StringBuilder();
      formatOperator(root, sb, 0);
      return sb.toString();
    }

    private static void formatOperator(LogicalOperator op, StringBuilder sb, int depth) {
      // The following assumes classes like DupElimLogicalOperator,
      // LogicalSortOperator, etc.
      // Adjust class checks as per your actual logical operator classes.
      if (op instanceof LogicalDuplicateEliminationOperator) {
        indent(sb, depth);
        sb.append("DupElim\n");
        formatChildren(op, sb, depth + 1);
      } else if (op instanceof operator.logical.LogicalSortOperator) {
        indent(sb, depth);
        operator.logical.LogicalSortOperator sortOp = (operator.logical.LogicalSortOperator) op;
        String cols =
            sortOp.getSortColumns().stream()
                .map(c -> c.getFullyQualifiedName())
                .collect(Collectors.joining(", "));
        sb.append("Sort[").append(cols).append("]\n");
        formatChildren(op, sb, depth + 1);
      } else if (op instanceof operator.logical.LogicalProjectOperator) {
        indent(sb, depth);
        LogicalProjectOperator projOp = (LogicalProjectOperator) op;
        String cols =
            projOp.getColumns().stream()
                .map(c -> c.getFullyQualifiedName())
                .collect(Collectors.joining(", "));
        sb.append("Project[").append(cols).append("]\n");
        formatChildren(op, sb, depth + 1);
      } else if (op instanceof operator.logical.LogicalJoinOperator) {
        indent(sb, depth);
        LogicalJoinOperator joinOp = (operator.logical.LogicalJoinOperator) op;
        sb.append("Join[").append(joinOp.getJoinCondition().toString()).append("]\n");

        // Print stats (if available)
        // For demonstration, we assume joinOp has a method getStats() that returns a
        // List<String>
        // Each string is something like: [[S.B, R.G], equals null, min null, max null]
        List<String> stats = joinOp.getStats();
        if (stats != null) {
          for (String stat : stats) {
            // According to the desired format, these stats lines appear at the same
            // indentation level as the join
            // The example shows them without dashes, directly after the join line.
            // We'll assume just print them as-is on new lines with no additional
            // indentation.
            sb.append(stat).append("\n");
          }
        }

        formatChildren(op, sb, depth + 1);
      } else if (op instanceof operator.logical.LogicalSelectOperator) {
        indent(sb, depth);
        operator.logical.LogicalSelectOperator selOp = (operator.logical.LogicalSelectOperator) op;
        sb.append("Select[").append(selOp.getCondition().toString()).append("]\n");
        formatChildren(op, sb, depth + 1);
      } else if (op instanceof operator.logical.LogicalScanOperator) {
        indent(sb, depth);
        operator.logical.LogicalScanOperator scanOp = (operator.logical.LogicalScanOperator) op;
        sb.append("Leaf[").append(scanOp.getTableName()).append("]\n");
        // no children to format
      }
    }

    private static void formatChildren(LogicalOperator op, StringBuilder sb, int depth) {
      List<LogicalOperator> children = op.getChildren();
      if (children != null) {
        for (LogicalOperator child : children) {
          formatOperator(child, sb, depth);
        }
      }
    }

    private static void indent(StringBuilder sb, int depth) {
      for (int i = 0; i < depth; i++) {
        sb.append(INDENT);
      }
    }
  }
}
