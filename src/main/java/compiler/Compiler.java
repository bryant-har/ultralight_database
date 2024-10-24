package compiler;

import common.DBCatalog;
import common.LogicalPlanBuilder;
import common.PhysicalPlanBuilder;
import file_management.TupleWriter;
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

    // Verify required directories and files exist
    verifyDirectoryStructure();

    // Initialize the database catalog with the schema
    DBCatalog.getInstance().setDataDirectory(inputDir + File.separator + "db");

    try {
      // Read queries from queries.sql
      String queriesPath = inputDir + File.separator + "queries.sql";
      String queriesContent = Files.readString(Paths.get(queriesPath));
      Statements statements = CCJSqlParserUtil.parseStatements(queriesContent);

      // Read physical plan builder configuration
      String configPath = inputDir + File.separator + "plan_builder_config.txt";

      // Create builders
      LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder();
      PhysicalPlanBuilder physicalPlanBuilder =
          new PhysicalPlanBuilder(logicalPlanBuilder.getTableAliases());

      // Process each query
      int queryCount = 1;
      for (Statement statement : statements.getStatements()) {
        logger.info("Processing query {}: {}", queryCount, statement);

        try {
          if (statement instanceof Select) {
            // Generate and execute query plan
            LogicalOperator logicalPlan = logicalPlanBuilder.buildPlan((Select) statement);
            logicalPlan.accept(physicalPlanBuilder);
            Operator physicalPlan = physicalPlanBuilder.getResult();

            // Write binary output to appropriate file
            String outputFile = outputDir + File.separator + "query" + queryCount;
            TupleWriter tw = null;
            try {
              tw = new TupleWriter(outputFile);
              physicalPlan.dump(tw);
            } catch (Exception e) {
              logger.error("Error writing output for query {}: {}", queryCount, e.getMessage());
              e.printStackTrace();
            } finally {
              if (tw != null) {
                try {
                  tw.close();
                } catch (Exception e) {
                  logger.error("Error closing TupleWriter for query {}: {}", queryCount, e.getMessage());
                }
              }
            }

            // old output not in binary
            // String outputFile = outputDir + File.separator + "query" + queryCount;
            // try (PrintStream output = new PrintStream(new File(outputFile))) {
            // physicalPlan.dump(output);

          } else {
            logger.warn("Skipping non-SELEC1T statement: {}", statement);
          }
        } catch (Exception e) {
          logger.error("Error processing query {}: {}", queryCount, e.getMessage());
          e.printStackTrace();
        }

        queryCount++;

      }

    } catch (

    Exception e) {
      logger.error("Fatal error during compilation: {}", e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void verifyDirectoryStructure() {
    // Verify input directory exists
    File inputDirFile = new File(inputDir);
    if (!inputDirFile.exists() || !inputDirFile.isDirectory()) {
      logger.error("Input directory does not exist: {}", inputDir);
      System.exit(1);
    }

    // Verify required input files exist
    File queriesFile = new File(inputDir, "queries.sql");
    File configFile = new File(inputDir, "plan_builder_config.txt");
    File dbDir = new File(inputDir, "db");
    File dataDir = new File(dbDir, "data");
    File schemaFile = new File(dbDir, "schema.txt");

    if (!queriesFile.exists()) {
      logger.error("queries.sql not found in input directory");
      System.exit(1);
    }
    if (!configFile.exists()) {
      logger.error("plan_builder_config.txt not found in input directory");
      System.exit(1);
    }
    if (!dbDir.exists() || !dbDir.isDirectory()) {
      logger.error("db directory not found in input directory");
      System.exit(1);
    }
    if (!dataDir.exists() || !dataDir.isDirectory()) {
      logger.error("data directory not found in db directory");
      System.exit(1);
    }
    if (!schemaFile.exists()) {
      logger.error("schema.txt not found in db directory");
      System.exit(1);
    }

    // Verify output directory exists
    File outputDirFile = new File(outputDir);
    if (!outputDirFile.exists() || !outputDirFile.isDirectory()) {
      logger.error("Output directory does not exist: {}", outputDir);
      System.exit(1);
    }

    // Verify temp directory exists
    File tempDirFile = new File(tempDir);
    if (!tempDirFile.exists() || !tempDirFile.isDirectory()) {
      logger.error("Temp directory does not exist: {}", tempDir);
      System.exit(1);
    }

    // Clean output directory
    File[] outputFiles = outputDirFile.listFiles();
    if (outputFiles != null) {
      for (File file : outputFiles) {
        file.delete();
      }
    }

    // Clean temp directory
    File[] tempFiles = tempDirFile.listFiles();
    if (tempFiles != null) {
      for (File file : tempFiles) {
        file.delete();
      }
    }
  }
}