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
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import operator.Operator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class QueryTest {

  private static final String INPUT_DIR = "src/test/resources/samples/input";
  private static final String EXPECTED_DIR = "src/test/resources/samples/expected";
  private static final String QUERIES_FILE = INPUT_DIR + "/queries.sql";

  @BeforeAll
  public static void setup() {
    DBCatalog.getInstance().setDataDirectory(INPUT_DIR + "/db");
  }

  @Test
  public void testQueries() throws Exception {
    String queries = Files.readString(Paths.get(QUERIES_FILE));
    Statements statements = CCJSqlParserUtil.parseStatements(queries);
    QueryPlanBuilder queryPlanBuilder = new QueryPlanBuilder();

    int counter = 1;
    for (Statement statement : statements.getStatements()) {
      // used to skip queries during testing, comment out to run all 14 commands
      if (counter == 13) {

        Operator plan = queryPlanBuilder.buildPlan(statement);
        List<Tuple> actualOutput = HelperMethods.collectAllTuples(plan);
        List<String> expectedOutput = readExpectedOutput(counter);
        List<String> actualOutputString =
            actualOutput.stream().map(Tuple::toString).collect(Collectors.toList());

        // check correct num of tuples
        assertEquals(
            expectedOutput.size(),
            actualOutputString.size(),
            "Query " + counter + " failed: Number of tuples do not match");

        System.out.println("Expected output: " + expectedOutput);
        // check correct content of tuples
        assertEquals(expectedOutput, actualOutputString, "Query " + counter + " failed");
      }
      counter++;
    }
  }

  private List<String> readExpectedOutput(int queryNumber) throws IOException {
    String expectedFilePath = EXPECTED_DIR + "/query" + queryNumber + "_humanreadable";
    return Files.readAllLines(Paths.get(expectedFilePath));
  }
}
