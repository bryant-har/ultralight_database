import static org.junit.jupiter.api.Assertions.assertEquals;

import common.DBCatalog;
import common.LogicalPlanBuilder;
import common.PhysicalPlanBuilder;
import common.Tuple;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import operator.logical.LogicalOperator;
import operator.physical.Operator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class P3UnitTests {
  private static final String INPUT_DIR = "src/test/resources/samples/input";
  private static final String EXPECTED_DIR = "src/test/resources/samples/expected_output_p3";
  private static final String QUERIES_FILE = INPUT_DIR + "/p3.sql";
  private static final String CONFIG_FILE = INPUT_DIR + "/plan_builder_config.txt";
  private static final Logger logger = LogManager.getLogger();
  private static final String DB_DIR = INPUT_DIR + "/db_p3";
  private static final String INDEX_DIR = DB_DIR + "/indexes";

  @BeforeAll
  public static void setup() {
    DBCatalog.getInstance().setDataDirectory(DB_DIR);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5})
  public void testQueries(int idx) throws Exception {
    String queries = Files.readString(Paths.get(QUERIES_FILE));
    List<Statement> statements = CCJSqlParserUtil.parseStatements(queries).getStatements();
    LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder();
    PhysicalPlanBuilder physicalPlanBuilder =
        new PhysicalPlanBuilder(logicalPlanBuilder.getTableAliases(), INDEX_DIR);

    Statement statement = statements.get(idx - 1);
    if (statement instanceof Select) {
      LogicalOperator logicalPlan = logicalPlanBuilder.buildPlan((Select) statement);

      long startTime = System.currentTimeMillis();
      logicalPlan.accept(physicalPlanBuilder);
      Operator physicalPlan = physicalPlanBuilder.getResult();
      long endTime = System.currentTimeMillis();

      List<Tuple> actualOutput = HelperMethods.collectAllTuples(physicalPlan);
      List<String> expectedOutput = readExpectedOutput(idx);
      List<String> actualOutputString =
          actualOutput.stream().map(Tuple::toString).collect(Collectors.toList());

      System.out.println("Time elapsed for query " + idx + ": " + (endTime - startTime) + "ms");
      logger.info("Time elapsed for query {}: {}ms", idx, (endTime - startTime));

      // Sort both outputs for comparison
      Collections.sort(expectedOutput);
      Collections.sort(actualOutputString);

      // Check correct number of tuples
      assertEquals(
          expectedOutput.size(),
          actualOutputString.size(),
          "Query " + idx + " failed: Number of tuples do not match");

      System.out.println("Expected output: " + expectedOutput);
      System.out.println("Actual Output: " + actualOutputString);

      assertEquals(expectedOutput, actualOutputString, "Query " + idx + " failed");
    } else {
      throw new UnsupportedOperationException("Only SELECT statements are supported");
    }
  }

  private List<String> readExpectedOutput(int queryNumber) throws IOException {
    String expectedFilePath = EXPECTED_DIR + "/query" + queryNumber + "_humanreadable";
    return Files.readAllLines(Paths.get(expectedFilePath));
  }
}
