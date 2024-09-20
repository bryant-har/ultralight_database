import common.DBCatalog;
import common.QueryPlanBuilder;
import common.Tuple;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import jdk.jshell.spi.ExecutionControl;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import operator.Operator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class P1UnitTests {
  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException, URISyntaxException {
    ClassLoader classLoader = P1UnitTests.class.getClassLoader();
    URI path = Objects.requireNonNull(classLoader.getResource("samples/input")).toURI();
    Path resourcePath = Paths.get(path);

    DBCatalog.getInstance().setDataDirectory(resourcePath.resolve("db").toString());

    URI queriesFile = Objects.requireNonNull(classLoader.getResource("samples/input/queries.sql")).toURI();

    statements = CCJSqlParserUtil.parseStatements(Files.readString(Paths.get(queriesFile)));
    queryPlanBuilder = new QueryPlanBuilder();
    statementList = statements.getStatements();
  }

  @Test
  public void testQuery1() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(0));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 200, 50))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50))),
        new Tuple(new ArrayList<>(List.of(5, 100, 500))),
        new Tuple(new ArrayList<>(List.of(6, 300, 400)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery2() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(1));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1))),
        new Tuple(new ArrayList<>(List.of(2))),
        new Tuple(new ArrayList<>(List.of(3))),
        new Tuple(new ArrayList<>(List.of(4))),
        new Tuple(new ArrayList<>(List.of(5))),
        new Tuple(new ArrayList<>(List.of(6)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery3() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(2));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1))),
        new Tuple(new ArrayList<>(List.of(2))),
        new Tuple(new ArrayList<>(List.of(3))),
        new Tuple(new ArrayList<>(List.of(4))),
        new Tuple(new ArrayList<>(List.of(5))),
        new Tuple(new ArrayList<>(List.of(6)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery4() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(3));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 2;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(Arrays.asList(1, 200, 50))),
        new Tuple(new ArrayList<>(Arrays.asList(2, 200, 200)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery5() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(4));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(Arrays.asList(1, 200, 50, 1, 101))),
        new Tuple(new ArrayList<>(Arrays.asList(1, 200, 50, 1, 102))),
        new Tuple(new ArrayList<>(Arrays.asList(1, 200, 50, 1, 103))),
        new Tuple(new ArrayList<>(Arrays.asList(2, 200, 200, 2, 101))),
        new Tuple(new ArrayList<>(Arrays.asList(3, 100, 105, 3, 102))),
        new Tuple(new ArrayList<>(Arrays.asList(4, 100, 50, 4, 104)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery6() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(
        statementList.get(5)); // Assuming statementList index 5 corresponds to query 6

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 15; // Total number of combinations given the values

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 2, 200, 200))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 3, 100, 105))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 4, 100, 50))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 5, 100, 500))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 6, 300, 400))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200, 3, 100, 105))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200, 4, 100, 50))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200, 5, 100, 500))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200, 6, 300, 400))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105, 4, 100, 50))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105, 5, 100, 500))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105, 6, 300, 400))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50, 5, 100, 500))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50, 6, 300, 400))),
        new Tuple(new ArrayList<>(List.of(5, 100, 500, 6, 300, 400)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuples[i], actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery7() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(6));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 4;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(Arrays.asList(1))),
        new Tuple(new ArrayList<>(Arrays.asList(2))),
        new Tuple(new ArrayList<>(Arrays.asList(3))),
        new Tuple(new ArrayList<>(Arrays.asList(4)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery8() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(7));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(Arrays.asList(3, 100, 105))),
        new Tuple(new ArrayList<>(Arrays.asList(4, 100, 50))),
        new Tuple(new ArrayList<>(Arrays.asList(5, 100, 500))),
        new Tuple(new ArrayList<>(Arrays.asList(1, 200, 50))),
        new Tuple(new ArrayList<>(Arrays.asList(2, 200, 200))),
        new Tuple(new ArrayList<>(Arrays.asList(6, 300, 400)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery9() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(8));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 0;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  @Test
  public void testQuery10() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(9));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 200, 50))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50))),
        new Tuple(new ArrayList<>(List.of(5, 100, 500))),
        new Tuple(new ArrayList<>(List.of(6, 300, 400)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery11() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(10));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 3;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 2))),
        new Tuple(new ArrayList<>(List.of(3, 2))),
        new Tuple(new ArrayList<>(List.of(2, 3)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery12() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(11));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1))),
        new Tuple(new ArrayList<>(List.of(2))),
        new Tuple(new ArrayList<>(List.of(3))),
        new Tuple(new ArrayList<>(List.of(4))),
        new Tuple(new ArrayList<>(List.of(5))),
        new Tuple(new ArrayList<>(List.of(6)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery13() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(12));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 5;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(3, 101))),
        new Tuple(new ArrayList<>(List.of(4, 102))),
        new Tuple(new ArrayList<>(List.of(2, 104))),
        new Tuple(new ArrayList<>(List.of(1, 103))),
        new Tuple(new ArrayList<>(List.of(8, 107)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery14() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(13));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 101))),
        new Tuple(new ArrayList<>(List.of(1, 102))),
        new Tuple(new ArrayList<>(List.of(1, 103))),
        new Tuple(new ArrayList<>(List.of(2, 101))),
        new Tuple(new ArrayList<>(List.of(3, 102))),
        new Tuple(new ArrayList<>(List.of(4, 104)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery15() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(14));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 3;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 200, 50))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery16() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(15));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 3;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1))),
        new Tuple(new ArrayList<>(List.of(2))),
        new Tuple(new ArrayList<>(List.of(4)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery17() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(16));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 0;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  @Test
  public void testQuery18() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(17));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 101))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 102))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 103))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200, 2, 101))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105, 3, 102))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50, 4, 104)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery19() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(18));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 101, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 102, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 103, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200, 2, 101, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105, 3, 102, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50, 4, 104, 104, 104, 2)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery20() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(19));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 2;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(3, 100, 105, 3, 102, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50, 4, 104, 104, 104, 2)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery21() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(20));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 200, 50))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50))),
        new Tuple(new ArrayList<>(List.of(5, 100, 500))),
        new Tuple(new ArrayList<>(List.of(6, 300, 400)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery22() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(21));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 15;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 2, 200, 200))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 3, 100, 105))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 4, 100, 50))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 5, 100, 500))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 6, 300, 400))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200, 3, 100, 105))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200, 4, 100, 50))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200, 5, 100, 500))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200, 6, 300, 400))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105, 4, 100, 50))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105, 5, 100, 500))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105, 6, 300, 400))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50, 5, 100, 500))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50, 6, 300, 400))),
        new Tuple(new ArrayList<>(List.of(5, 100, 500, 6, 300, 400)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery23() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(22));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 5;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(3, 101))),
        new Tuple(new ArrayList<>(List.of(4, 102))),
        new Tuple(new ArrayList<>(List.of(1, 103))),
        new Tuple(new ArrayList<>(List.of(2, 104))),
        new Tuple(new ArrayList<>(List.of(8, 107)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery24() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(23));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 101, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 102, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 103, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50, 4, 104, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105, 3, 102, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200, 2, 101, 101, 2, 3)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery25() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(24));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 101, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 102, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 103, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50, 4, 104, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105, 3, 102, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200, 2, 101, 101, 2, 3)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery26() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(25));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 5;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 200, 50))),
        new Tuple(new ArrayList<>(List.of(4, 100, 50))),
        new Tuple(new ArrayList<>(List.of(3, 100, 105))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200))),
        new Tuple(new ArrayList<>(List.of(5, 100, 500)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery27() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(26));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 4;
    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 101, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 102, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(1, 200, 50, 1, 103, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(2, 200, 200, 2, 101, 101, 2, 3)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery28() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(27));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 3;
    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 1))),
        new Tuple(new ArrayList<>(List.of(2, 2))),
        new Tuple(new ArrayList<>(List.of(6, 6)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery29() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(28));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 2;
    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(3, 3))),
        new Tuple(new ArrayList<>(List.of(4, 4)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery30() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(29));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;
    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 1))),
        new Tuple(new ArrayList<>(List.of(1, 1))),
        new Tuple(new ArrayList<>(List.of(1, 1))),
        new Tuple(new ArrayList<>(List.of(2, 2))),
        new Tuple(new ArrayList<>(List.of(3, 3))),
        new Tuple(new ArrayList<>(List.of(4, 4)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery31() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(30));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 4;
    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1))),
        new Tuple(new ArrayList<>(List.of(2))),
        new Tuple(new ArrayList<>(List.of(3))),
        new Tuple(new ArrayList<>(List.of(4)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery32() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(31));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;
    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 1))),
        new Tuple(new ArrayList<>(List.of(1, 1))),
        new Tuple(new ArrayList<>(List.of(1, 1))),
        new Tuple(new ArrayList<>(List.of(2, 2))),
        new Tuple(new ArrayList<>(List.of(3, 3))),
        new Tuple(new ArrayList<>(List.of(4, 4)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

}
