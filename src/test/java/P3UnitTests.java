import static org.junit.jupiter.api.Assertions.assertEquals;

import common.BulkLoader;
import common.DBCatalog;
import common.LogicalPlanBuilder;
import common.PhysicalPlanBuilder;
import common.Tuple;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import operator.logical.LogicalOperator;
import operator.physical.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class P3UnitTests {
  private static final String INPUT_DIR = "src/test/resources/samples/input";
  private static final String EXPECTED_DIR = "src/test/resources/samples/expected_p3";
  private static final String QUERIES_FILE = INPUT_DIR + "/p3.sql";
  private static final String CONFIG_FILE = INPUT_DIR + "/plan_builder_config.txt";
  private static final String DB_DIR = INPUT_DIR + "/db_p3";
  private static final String INDEX_DIR = DB_DIR + "/indexes";

  @BeforeAll
  public static void setup() {
    DBCatalog.getInstance().setDataDirectory(DB_DIR);
    System.out.println("Database directory set to: " + DB_DIR);
    new File(INDEX_DIR).mkdirs();
  }

  @BeforeEach
  public void setupIndexes() {
    File indexDir = new File(INDEX_DIR);
    if (indexDir.exists()) {
      for (File file : indexDir.listFiles()) {
        file.delete();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = { 3 })
  public void testQueriesWithoutIndexes(int idx) throws Exception {
    runTest(idx, false);
  }

  @Disabled
  @ParameterizedTest
  @ValueSource(ints = { 1, 3 })
  public void testQueriesWithIndexes(int idx) throws Exception {
    buildIndexes();
    runTest(idx, true);
  }

  private void debugPhysicalPlan(Operator physicalPlan, Statement statement) {
    System.out.println("\nDEBUG: Physical Plan for query: " + statement);
    debugOperator(physicalPlan, 0);
  }

  private void debugOperator(Operator op, int depth) {
    String indent = "  ".repeat(depth);
    System.out.println(indent + "Operator: " + op.getClass().getSimpleName());

    if (op instanceof SelectOperator) {
      System.out.println(indent + "Selection condition exists");
    } else if (op instanceof IndexScanOperator) {
      System.out.println(indent + "Index scan operator found");
    } else if (op instanceof ScanOperator) {
      System.out.println(indent + "Table scan operator found");
    }

    // Print schema information
    System.out.println(indent + "Output schema: " + op.getOutputSchema().stream()
        .map(col -> col.getTable().getName() + "." + col.getColumnName())
        .collect(Collectors.joining(", ")));
  }

  private void buildIndexes() throws IOException {
    String indexInfoPath = DB_DIR + "/index_info.txt";

    try (BufferedReader reader = new BufferedReader(new FileReader(indexInfoPath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split("\\s+");
        if (parts.length >= 4) {
          String tableName = parts[0];
          String columnName = parts[1];
          String outputFile = INDEX_DIR + File.separator + tableName + "." + columnName;

          try {
            BulkLoader loader = new BulkLoader(indexInfoPath, outputFile);
            loader.buildAndSerialize();

            try (RandomAccessFile raf = new RandomAccessFile(outputFile, "r")) {
              int rootAddr = raf.readInt();
              int numLeaves = raf.readInt();
              int order = raf.readInt();
              System.out.println(String.format(
                  "Index %s.%s header: RootAddr=%d, NumLeaves=%d, Order=%d",
                  tableName, columnName, rootAddr, numLeaves, order));
            }
          } catch (Exception e) {
            System.err.println("Error building index for " + tableName + "." + columnName + ": " + e.getMessage());
            e.printStackTrace();
            throw e;
          }
        }
      }
    }
  }

  private void runTest(int idx, boolean useIndexes) throws Exception {
    System.out.println("\nExecuting Query " + idx + (useIndexes ? " with" : " without") + " indexes");

    String queries = Files.readString(Paths.get(QUERIES_FILE));
    List<Statement> statements = CCJSqlParserUtil.parseStatements(queries).getStatements();

    LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder();
    PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(
        logicalPlanBuilder.getTableAliases(),
        INDEX_DIR);

    Statement statement = statements.get(idx - 1);
    System.out.println("Executing query: " + statement.toString());

    if (statement instanceof Select) {
      LogicalOperator logicalPlan = logicalPlanBuilder.buildPlan((Select) statement);
      logicalPlan.accept(physicalPlanBuilder);
      Operator physicalPlan = physicalPlanBuilder.getResult();

      // Debug physical plan before execution
      debugPhysicalPlan(physicalPlan, statement);

      List<Tuple> actualOutput = HelperMethods.collectAllTuples(physicalPlan);
      List<String> actualOutputString = actualOutput.stream()
          .map(Tuple::toString)
          .collect(Collectors.toList());

      List<String> expectedOutput = readExpectedOutput(idx);

      System.out.println("Expected output size: " + expectedOutput.size());
      System.out.println("Actual output size: " + actualOutputString.size());
      System.out.println(actualOutput);

      if (expectedOutput.size() != actualOutputString.size()) {
        System.out.println("First few expected tuples: " +
            expectedOutput.subList(0, Math.min(5, expectedOutput.size())));
        System.out.println("First few actual tuples: " +
            actualOutputString.subList(0, Math.min(5, actualOutputString.size())));
      }

      assertEquals(expectedOutput.size(), actualOutputString.size(),
          String.format("Query %d (indexes: %b) failed: Number of tuples do not match",
              idx, useIndexes));

      assertEquals(expectedOutput, actualOutputString,
          String.format("Query %d (indexes: %b) failed: Content does not match",
              idx, useIndexes));
    } else {
      throw new UnsupportedOperationException("Only SELECT statements are supported");
    }

  }

  private List<String> readExpectedOutput(int queryNumber) throws IOException {
    String expectedFilePath = EXPECTED_DIR + "/query" + queryNumber + "_humanreadable";
    return Files.readAllLines(Paths.get(expectedFilePath));
  }
}