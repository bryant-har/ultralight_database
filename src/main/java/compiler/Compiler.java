package compiler;

import common.DBCatalog;
import common.LogicalPlanBuilder;
import common.PhysicalPlanBuilder;
import file_management.TupleWriter;
import java.io.File;
import java.io.IOException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.Select;
import operator.logical.LogicalOperator;
import operator.physical.Operator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Compiler {
  private static final Logger logger = LogManager.getLogger();
  private static String inputDir;
  private static String outputDir;
  private static String tempDir;

  public static void main(String[] args) {
    // Validate command line arguments
    if (args.length != 3) {
      logger.error("Usage: java -jar program.jar inputdir outputdir tempdir");
      System.exit(1);
    }

    // Set directories from command line arguments
    inputDir = args[0];
    outputDir = args[1];
    tempDir = args[2];

    try {
      // Initialize database catalog with schema
      String dbDir = inputDir + File.separator + "db";
      DBCatalog.getInstance().setDataDirectory(dbDir);

      // Read queries from queries.sql
      String queriesPath = inputDir + File.separator + "queries.sql";
      String queriesContent =
          new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(queriesPath)));
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
          // Generate and execute query plan
          LogicalOperator logicalPlan = logicalPlanBuilder.buildPlan((Select) statement);
          logicalPlan.accept(physicalPlanBuilder);
          Operator physicalPlan = physicalPlanBuilder.getResult();

          // Write query plans
          writeQueryPlan(queryCount, "logicalplan", logicalPlan.toString());
          writeQueryPlan(queryCount, "physicalplan", physicalPlan.toString());

          // Write results
          String outputFile = outputDir + File.separator + "query" + queryCount;
          TupleWriter writer = null;
          try {
            writer = new TupleWriter(outputFile);
            physicalPlan.dump(writer);
          } catch (Exception e) {
            logger.error("Error writing output for query {}: {}", queryCount, e.getMessage());
            e.printStackTrace();
          } finally {
            if (writer != null) {
              try {
                writer.close();
              } catch (IOException e) {
                logger.error("Error closing writer for query {}: {}", queryCount, e.getMessage());
              }
            }
          }
        }
        queryCount++;
      }

    } catch (Exception e) {
      logger.error("Error during compilation: ", e);
      System.exit(1);
    }
  }

  private static void writeQueryPlan(int queryNum, String planType, String content)
      throws IOException {
    String filename = outputDir + File.separator + "query" + queryNum + "_" + planType;
    java.nio.file.Files.write(java.nio.file.Paths.get(filename), content.getBytes());
  }
}
