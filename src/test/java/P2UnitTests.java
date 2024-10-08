import static org.junit.jupiter.api.Assertions.assertEquals;

import common.DBCatalog;
import common.QueryPlanBuilder;
import common.Tuple;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statements;
import operator.physical.Operator;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class P2UnitTests {

  private static final String INPUT_DIR = "src/test/resources/samples/input";
  private static final String EXPECTED_DIR = "src/test/resources/samples/expected";
  private static final String QUERIES_FILE = INPUT_DIR + "/p2.sql";

  @BeforeAll
  public static void setup() {
    DBCatalog.getInstance().setDataDirectory(INPUT_DIR + "/db_p2");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
  public void testQueries(int idx) throws Exception {
    String queries = Files.readString(Paths.get(QUERIES_FILE));
    Statements statements = CCJSqlParserUtil.parseStatements(queries);
    QueryPlanBuilder queryPlanBuilder = new QueryPlanBuilder();

    Operator plan = queryPlanBuilder.buildPlan(statements.getStatements().get(idx - 1));
    List<Tuple> actualOutput = HelperMethods.collectAllTuples(plan);
    List<String> expectedOutput = readExpectedOutput(idx);
    List<String> actualOutputString =
        actualOutput.stream().map(Tuple::toString).collect(Collectors.toList());

    // check correct num of tuples
    assertEquals(
        expectedOutput.size(),
        actualOutputString.size(),
        "Query " + idx + " failed: Number of tuples do not match");

    System.out.println("Expected output: " + expectedOutput);
    // check correct content of tuples
    assertEquals(expectedOutput, actualOutputString, "Query " + idx + " failed");
  }

  private List<String> readExpectedOutput(int queryNumber) throws IOException {
    String expectedFilePath = EXPECTED_DIR + "/query" + queryNumber + "_humanreadable";
    return Files.readAllLines(Paths.get(expectedFilePath));
  }
}
