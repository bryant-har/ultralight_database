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

public class P2UnitTests {
  private static final String INPUT_DIR = "src/test/resources/samples/input";
  private static final String EXPECTED_DIR = "src/test/resources/samples/expected";
  private static final String QUERIES_FILE = INPUT_DIR + "/p2.sql";
  private static final String CONFIG_FILE = INPUT_DIR + "/plan_builder_config.txt";
  private static final Logger logger = LogManager.getLogger();


  @BeforeAll
  public static void setup() {
    DBCatalog.getInstance().setDataDirectory(INPUT_DIR + "/db_p2");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
  public void testQueries(int idx) throws Exception {
    String queries = Files.readString(Paths.get(QUERIES_FILE));
    List<Statement> statements = CCJSqlParserUtil.parseStatements(queries).getStatements();

    LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder();
    PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(
        logicalPlanBuilder.getTableAliases(), CONFIG_FILE, INPUT_DIR + "/temp");

    Statement statement = statements.get(idx - 1);
    if (statement instanceof Select) {

      LogicalOperator logicalPlan = logicalPlanBuilder.buildPlan((Select) statement);
      long startTime = System.currentTimeMillis();
      logicalPlan.accept(physicalPlanBuilder);
      Operator physicalPlan = physicalPlanBuilder.getResult();
      long endTime = System.currentTimeMillis();

      List<Tuple> actualOutput = HelperMethods.collectAllTuples(physicalPlan);
      
      List<String> expectedOutput = readExpectedOutput(idx);
      List<String> actualOutputString = actualOutput.stream().map(Tuple::toString).collect(Collectors.toList());

      // log the time the elasped from AFTER plan is built to BEFORE the output is collected
      System.out.println("Time elapsed for query " + idx + ": " + (endTime - startTime) + "ms");
      logger.info("Time elapsed for query {}: {}ms", idx, (endTime - startTime));


      // check correct num of tuples
      assertEquals(
          expectedOutput.size(),
          actualOutputString.size(),
          "Query " + idx + " failed: Number of tuples do not match");

      System.out.println("Expected output: " + expectedOutput);
      System.out.println("Actual Output: " + actualOutputString);
      // check correct content of tuples
      Collections.sort(expectedOutput);
      Collections.sort(actualOutputString);
      assertEquals(expectedOutput, actualOutputString, "Query " + idx + " failed");
    } else {
      throw new UnsupportedOperationException("Only SsELECT statements are supported");
    }
  }

  private List<String> readExpectedOutput(int queryNumber) throws IOException {
    String expectedFilePath = EXPECTED_DIR + "/query" + queryNumber +
        "_humanreadable";
    // String expectedFilePath = EXPECTED_DIR + "/query" + queryNumber;
    return Files.readAllLines(Paths.get(expectedFilePath));
  }
}
