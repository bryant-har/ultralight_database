import static org.junit.jupiter.api.Assertions.assertEquals;

import common.DBCatalog;
import common.LogicalPlanBuilder;
import common.PhysicalPlanBuilder;
import common.Tuple;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class P3UnitTests {
  private static final String INPUT_DIR = "src/test/resources/samples/input";
  private static final String EXPECTED_DIR = "src/test/resources/samples/expected";
  private static final String QUERIES_FILE = INPUT_DIR + "/p2.sql";
  private static final String CONFIG_FILE = INPUT_DIR + "/plan_builder_config.txt";
  private static final String DB_DIR = INPUT_DIR + "/db_p2";

  @BeforeAll
  public static void setup() {
    // Set up to use P2 database
    DBCatalog.getInstance().setDataDirectory(DB_DIR);
    System.out.println("Database directory set to: " + DB_DIR);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
  public void testQueriesWithoutIndexes(int idx) throws Exception {
    runTest(idx, false);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
  public void testQueriesWithIndexes(int idx) throws Exception {
    runTest(idx, true);
  }

  private void runTest(int idx, boolean useIndexes) throws Exception {
    System.out.println(
        "\nExecuting Query " + idx + (useIndexes ? " with" : " without") + " indexes");

    String queries = Files.readString(Paths.get(QUERIES_FILE));
    List<Statement> statements = CCJSqlParserUtil.parseStatements(queries).getStatements();

    LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder();
    PhysicalPlanBuilder physicalPlanBuilder =
        new PhysicalPlanBuilder(logicalPlanBuilder.getTableAliases(), useIndexes, DB_DIR);

    Statement statement = statements.get(idx - 1);
    System.out.println("Executing query: " + statement.toString());

    if (statement instanceof Select) {
      LogicalOperator logicalPlan = logicalPlanBuilder.buildPlan((Select) statement);
      logicalPlan.accept(physicalPlanBuilder);
      Operator physicalPlan = physicalPlanBuilder.getResult();

      List<Tuple> actualOutput = HelperMethods.collectAllTuples(physicalPlan);
      List<String> actualOutputString =
          actualOutput.stream().map(Tuple::toString).collect(Collectors.toList());

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
