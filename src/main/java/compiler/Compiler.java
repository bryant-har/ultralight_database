package compiler;

import common.DBCatalog;
import common.LogicalPlanBuilder;
import common.PhysicalPlanBuilder;
import common.StatsMaker;
import file_management.TupleWriter;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.Select;
import operator.logical.LogicalOperator;
import operator.physical.Operator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Main compiler class that handles query optimization and processing. Implements statistics
 * gathering, selection pushing, and optimized join ordering for Projects 2/3/4.
 */
public class Compiler {
  private static final Logger logger = LogManager.getLogger(Compiler.class);
  private static String inputDir;
  private static String outputDir;
  private static String tempDir;

  public static void main(String[] args) {
    // Read configuration file containing input, output and temp directories
    if (args.length != 1) {
      logger.error("Usage: java -jar program.jar config_file");
      System.exit(1);
    }

    try {
      // Read directories from config file
      readConfig(args[0]);

      // Initialize database directory path
      String dbDir = inputDir + File.separator + "db";

      // Initialize catalog and gather statistics
      DBCatalog.getInstance().setDataDirectory(dbDir);

      // Create statistics file
      StatsMaker statsMaker = new StatsMaker(dbDir);
      statsMaker.createStats();

      // Read queries
      String queriesPath = inputDir + File.separator + "queries.sql";
      String queriesContent = new String(Files.readAllBytes(Paths.get(queriesPath)));
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
          processQuery((Select) statement, queryCount++, logicalPlanBuilder, physicalPlanBuilder);
        }
      }

    } catch (Exception e) {
      logger.error("Error during compilation: ", e);
      e.printStackTrace();
      System.exit(1);
    }
  }

  /** Reads the configuration file containing input, output and temp directories. */
  private static void readConfig(String configPath) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(configPath))) {
      inputDir = reader.readLine();
      outputDir = reader.readLine();
      tempDir = reader.readLine();
    }
  }

  /** Processes a single query by building logical and physical plans and executing it. */
  private static void processQuery(
      Select select,
      int queryNumber,
      LogicalPlanBuilder logicalPlanBuilder,
      PhysicalPlanBuilder physicalPlanBuilder)
      throws IOException {

    // Generate logical plan with selection pushing
    LogicalOperator logicalPlan = logicalPlanBuilder.buildPlan(select);

    // Accept the visitor to build physical plan with optimized join ordering
    logicalPlan.accept(physicalPlanBuilder);
    Operator physicalPlan = physicalPlanBuilder.getResult();

    // Write query plans
    writeQueryPlan(queryNumber, "logicalplan", logicalPlan.toString());
    writeQueryPlan(queryNumber, "physicalplan", physicalPlan.toString());

    // Write query results
    String outputFile = outputDir + File.separator + "query" + queryNumber;
    TupleWriter writer = null;
    try {
      writer = new TupleWriter(outputFile);
      physicalPlan.dump(writer);
    } catch (Exception e) {
      logger.error("Error writing output for query {}: {}", queryNumber, e.getMessage());
      throw new IOException(e);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          logger.error("Error closing writer for query {}: {}", queryNumber, e.getMessage());
        }
      }
    }
  }

  /** Writes a query plan to a file. */
  private static void writeQueryPlan(int queryNum, String planType, String content)
      throws IOException {
    String filename = outputDir + File.separator + "query" + queryNum + "_" + planType;
    Files.write(Paths.get(filename), content.getBytes());
  }
}
