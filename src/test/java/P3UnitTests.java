
import static org.junit.jupiter.api.Assertions.assertEquals;

import common.DBCatalog;
import common.LogicalPlanBuilder;
import common.PhysicalPlanBuilder;
import common.Tuple;
import common.BulkLoader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import operator.logical.LogicalOperator;
import operator.physical.Operator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class P3UnitTests {
  private static final String INPUT_DIR = "src/test/resources/samples/input";
  private static final String EXPECTED_DIR = "src/test/resources/samples/expected";
  private static final String QUERIES_FILE = INPUT_DIR + "/p2.sql";
  private static final String CONFIG_FILE = INPUT_DIR + "/plan_builder_config.txt";
  private static final String DB_DIR = INPUT_DIR + "/db_p2";
  private static final String INDEX_DIR = DB_DIR + "/indexes";

  @BeforeAll
  public static void setup() {
    // Set up to use P3 database
    DBCatalog.getInstance().setDataDirectory(DB_DIR);
    System.out.println("Database directory set to: " + DB_DIR);

    // Create index directory if it doesn't exist
    new File(INDEX_DIR).mkdirs();
  }

  @BeforeEach
  public void setupIndexes() {
    // Clean any existing indexes
    File indexDir = new File(INDEX_DIR);
    if (indexDir.exists()) {
      for (File file : indexDir.listFiles()) {
        file.delete();
      }
    }
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
          boolean isClustered = parts[2].equals("1");
          int order = Integer.parseInt(parts[3]);

          String outputFile = INDEX_DIR + File.separator + tableName + "." + columnName;

          try {
            // Create and use BulkLoader to build the index
            BulkLoader loader = new BulkLoader(indexInfoPath, outputFile);
            loader.buildAndSerialize();
            System.out.println("Built index for " + tableName + "." + columnName);
          } catch (Exception e) {
            System.err.println("Error building index for " + tableName + "." + columnName + ": " + e.getMessage());
            e.printStackTrace();
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 })
  public void testQueriesWithoutIndexes(int idx) throws Exception {
    runTest(idx, false);
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 })
  public void testQueriesWithIndexes(int idx) throws Exception {
    // Build indexes before running tests with indexes
    buildIndexes();
    runTest(idx, true);
  }

  private void runTest(int idx, boolean useIndexes) throws Exception {
    System.out.println(
        "\nExecuting Query " + idx + (useIndexes ? " with" : " without") + " indexes");

    String queries = Files.readString(Paths.get(QUERIES_FILE));
    List<Statement> statements = CCJSqlParserUtil.parseStatements(queries).getStatements();

    LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder();
    PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(
        logicalPlanBuilder.getTableAliases(),
        INDEX_DIR // Pass the index directory path
    );

    Statement statement = statements.get(idx - 1);
    System.out.println("Executing query: " + statement.toString());

    if (statement instanceof Select) {
      LogicalOperator logicalPlan = logicalPlanBuilder.buildPlan((Select) statement);
      logicalPlan.accept(physicalPlanBuilder);
      Operator physicalPlan = physicalPlanBuilder.getResult();

      List<Tuple> actualOutput = HelperMethods.collectAllTuples(physicalPlan);
      List<String> actualOutputString = actualOutput.stream().map(Tuple::toString).collect(Collectors.toList());

      List<String> expectedOutput = readExpectedOutput(idx);

      // Debug output
      System.out.println("Expected output size: " + expectedOutput.size());
      System.out.println("Actual output size: " + actualOutputString.size());

      if (expectedOutput.size() != actualOutputString.size()) {
        System.out.println(
            "First few expected tuples: "
                + expectedOutput.subList(0, Math.min(5, expectedOutput.size())));
        System.out.println(
            "First few actual tuples: "
                + actualOutputString.subList(0, Math.min(5, actualOutputString.size())));
      }

      assertEquals(
          expectedOutput.size(),
          actualOutputString.size(),
          String.format(
              "Query %d (indexes: %b) failed: Number of tuples do not match", idx, useIndexes));

      assertEquals(
          expectedOutput,
          actualOutputString,
          String.format("Query %d (indexes: %b) failed: Content does not match", idx, useIndexes));
    } else {
      throw new UnsupportedOperationException("Only SELECT statements are supported");
    }
  }

  private List<String> readExpectedOutput(int queryNumber) throws IOException {
    String expectedFilePath = EXPECTED_DIR + "/query" + queryNumber + "_humanreadable";
    return Files.readAllLines(Paths.get(expectedFilePath));
  }
}