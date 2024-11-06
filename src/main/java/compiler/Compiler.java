package compiler;

import common.DBCatalog;
import common.LogicalPlanBuilder;
import common.PhysicalPlanBuilder;
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
import org.apache.logging.log4j.*;

public class Compiler {
  private static final Logger logger = LogManager.getLogger();
  private static String inputDir;
  private static String outputDir;
  private static String tempDir;
  private static boolean buildIndexes;
  private static boolean evaluateQueries;
  private static boolean useIndexes;

  public static void main(String[] args) {
    // Validate command line arguments - now expects config file path
    if (args.length != 1) {
      logger.error("Usage: java -jar program.jar config_file_path");
      System.exit(1);
    }

    // Read configuration from file
    try {
      readConfig(args[0]);
    } catch (IOException e) {
      logger.error("Error reading configuration file: " + e.getMessage());
      System.exit(1);
    }

    // Verify required directories and files exist
    verifyDirectoryStructure();

    // Initialize the database catalog with the schema
    DBCatalog.getInstance().setDataDirectory(inputDir + File.separator + "db");

    try {
      // If indexes need to be built, do that first
      if (buildIndexes) {
        buildIndexes();
      }

      // If queries should not be evaluated, exit here
      if (!evaluateQueries) {
        logger.info("Index building completed. Query evaluation disabled.");
        return;
      }

      // Read plan builder configuration
      String configPath = inputDir + File.separator + "plan_builder_config.txt";
      useIndexes = readPlanBuilderConfig(configPath);

      // Read queries from queries.sql
      String queriesPath = inputDir + File.separator + "queries.sql";
      String queriesContent = Files.readString(Paths.get(queriesPath));
      Statements statements = CCJSqlParserUtil.parseStatements(queriesContent);

      // Create builders
      LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder();
      String dbPath = inputDir + File.separator + "db";
      PhysicalPlanBuilder physicalPlanBuilder =
          new PhysicalPlanBuilder(logicalPlanBuilder.getTableAliases(), useIndexes, dbPath);

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

            // Write binary output
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
                  logger.error(
                      "Error closing TupleWriter for query {}: {}", queryCount, e.getMessage());
                }
              }
            }
          } else {
            logger.warn("Skipping non-SELECT statement: {}", statement);
          }
        } catch (Exception e) {
          logger.error("Error processing query {}: {}", queryCount, e.getMessage());
          e.printStackTrace();
        }

        queryCount++;
      }

    } catch (Exception e) {
      logger.error("Fatal error during compilation: {}", e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void readConfig(String configPath) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(configPath))) {
      inputDir = reader.readLine();
      outputDir = reader.readLine();
      tempDir = reader.readLine();
      buildIndexes = reader.readLine().equals("1");
      evaluateQueries = reader.readLine().equals("1");
    }
  }

  private static boolean readPlanBuilderConfig(String configPath) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(configPath))) {
      reader.readLine(); // Skip first line
      reader.readLine(); // Skip second line
      return reader.readLine().equals("1"); // Third line indicates whether to use indexes
    }
  }

  private static void buildIndexes() throws IOException {
    logger.info("Building indexes...");

    // Read index_info.txt to determine which indexes to build
    String indexInfoPath = inputDir + File.separator + "db" + File.separator + "index_info.txt";

    // Create indexes directory if it doesn't exist
    File indexesDir = new File(inputDir + File.separator + "db" + File.separator + "indexes");
    if (!indexesDir.exists()) {
      indexesDir.mkdirs();
    }

    try (BufferedReader reader = new BufferedReader(new FileReader(indexInfoPath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split("\\s+");
        if (parts.length >= 4) {
          String tableName = parts[0];
          String columnName = parts[1];
          boolean isClustered = parts[2].equals("1");
          int order = Integer.parseInt(parts[3]);

          // Build the index for this table
          String outputFile = indexesDir.getPath() + File.separator + tableName + "." + columnName;

          // Create and use BulkLoader to build the index
          try {
            common.BulkLoader loader = new common.BulkLoader(indexInfoPath, outputFile);
            loader.buildAndSerialize();
            logger.info("Built index for {}.{}", tableName, columnName);
          } catch (Exception e) {
            logger.error(
                "Error building index for {}.{}: {}", tableName, columnName, e.getMessage());
            e.printStackTrace();
          }
        }
      }
    }

    logger.info("Index building completed.");
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
    File indexInfoFile = new File(dbDir, "index_info.txt");

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
    if (!indexInfoFile.exists()) {
      logger.error("index_info.txt not found in db directory");
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
