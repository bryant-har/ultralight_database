package compiler;

import common.DBCatalog;
import common.LogicalPlanBuilder;
import common.PhysicalPlanBuilder;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.Select;
import operator.logical.LogicalOperator;
import operator.physical.Operator;
import org.apache.logging.log4j.*;

public class Compiler {
  private static final Logger logger = LogManager.getLogger();

  private static String outputDir;
  private static String inputDir;
  private static final boolean outputToFiles = true;

  public static void main(String[] args) {
    inputDir = args[0];
    outputDir = args[1];
    DBCatalog.getInstance().setDataDirectory(inputDir + "/db");

    try {
      String str = Files.readString(Paths.get(inputDir + "/queries.sql"));
      Statements statements = CCJSqlParserUtil.parseStatements(str);

      // Create instances of LogicalPlanBuilder and PhysicalPlanBuilder
      LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder();
      PhysicalPlanBuilder physicalPlanBuilder =
          new PhysicalPlanBuilder(logicalPlanBuilder.getTableAliases());

      if (outputToFiles) {
        for (File file : (new File(outputDir).listFiles())) file.delete();
      }

      int counter = 1;
      for (Statement statement : statements.getStatements()) {
        logger.info("Processing query: " + statement);

        try {
          if (statement instanceof Select) {
            // Generate logical plan
            LogicalOperator logicalPlan = logicalPlanBuilder.buildPlan((Select) statement);

            // Generate physical plan from logical plan
            logicalPlan.accept(physicalPlanBuilder);
            Operator physicalPlan = physicalPlanBuilder.getResult();

            if (outputToFiles) {
              File outfile = new File(outputDir + "/query" + counter);
              physicalPlan.dump(new PrintStream(outfile));
            } else {
              physicalPlan.dump(System.out);
            }
          } else {
            logger.warn("Skipping non-SELECT statement: " + statement);
          }
        } catch (Exception e) {
          logger.error("Error processing query " + counter + ": " + e.getMessage(), e);
        }

        ++counter;
      }
    } catch (Exception e) {
      logger.error("Exception occurred in interpreter: " + e.getMessage(), e);
    }
  }
}
