package compiler;

import common.BulkLoader;
import common.DBCatalog;
import common.LogicalPlanBuilder;
import common.PhysicalPlanBuilder;
import file_management.TupleReader;
import file_management.TupleWriter;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.Select;
import operator.logical.LogicalOperator;
import operator.physical.Operator;
import org.apache.logging.log4j.*;

/**
 * Main compiler class that handles query processing and index management. Supports both query
 * evaluation and index building based on configuration.
 */
public class Compiler {
  private static final Logger logger = LogManager.getLogger();
  private static String inputDir;
  private static String outputDir;
  private static String tempDir;
  private static boolean buildIndexes;
  private static boolean evaluateQueries;
  private static boolean useIndexes;

  /**
   * Main entry point for the database compiler. Handles index building and query evaluation based
   * on configuration.
   *
   * @param args Command line arguments - expects path to config file
   */
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
        System.out.println("build indexes");
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
      String indexDir = inputDir + File.separator + "db" + File.separator + "indexes";
      PhysicalPlanBuilder physicalPlanBuilder =
          new PhysicalPlanBuilder(logicalPlanBuilder.getTableAliases(), indexDir);

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
                } catch (IOException e) {
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

  /**
   * Reads configuration from the provided config file.
   *
   * @param configPath Path to the configuration file
   * @throws IOException If there is an error reading the file
   */
  private static void readConfig(String configPath) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(configPath))) {
      inputDir = reader.readLine();
      outputDir = reader.readLine();
      tempDir = reader.readLine();
      buildIndexes = reader.readLine().equals("1");
      evaluateQueries = reader.readLine().equals("1");
    }
  }

  /**
   * Reads the plan builder configuration file to determine whether to use indexes.
   *
   * @param configPath Path to the plan builder config file
   * @return true if indexes should be used, false otherwise
   * @throws IOException If there is an error reading the file
   */
  private static boolean readPlanBuilderConfig(String configPath) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(configPath))) {
      reader.readLine(); // Skip first line
      reader.readLine(); // Skip second line
      return reader.readLine().equals("1"); // Third line indicates whether to use indexes
    }
  }

  /**
   * Builds indexes based on the index_info.txt configuration. Handles both clustered and
   * unclustered indexes.
   *
   * @throws IOException If there is an error reading or writing files
   */
  /**
   * Builds indexes based on the index_info.txt configuration. This method retains the manual
   * sorting for clustered indexes from the original code, but uses the BulkLoader approach from the
   * P3UnitTests to construct indexes.
   *
   * @throws IOException If there is an error reading or writing files
   */
  private static void buildIndexes() throws IOException {
    logger.info("Building indexes...");
    String indexInfoPath = inputDir + File.separator + "db" + File.separator + "index_info.txt";
    String indexDir = inputDir + File.separator + "db" + File.separator + "indexes";
    String dataDir = inputDir + File.separator + "db" + File.separator + "data";

    // Create indexes directory if it doesn't exist
    new File(indexDir).mkdirs();

    List<String> indexConfigs = Files.readAllLines(Paths.get(indexInfoPath));
    int totalIndexes = indexConfigs.size();
    int currentIndex = 0;

    logger.info("Found {} indexes to build", totalIndexes);

    for (String line : indexConfigs) {
      String[] parts = line.split("\\s+");
      if (parts.length >= 4) {
        String tableName = parts[0];
        String columnName = parts[1];
        boolean isClustered = parts[2].equals("1");
        int order = Integer.parseInt(parts[3]);

        currentIndex++;
        logger.info(
            "Building index {}/{}: {}.{} ({})",
            currentIndex,
            totalIndexes,
            tableName,
            columnName,
            isClustered ? "clustered" : "unclustered");

        String outputFile = indexDir + File.separator + tableName + "." + columnName;

        try {
          // If the index is clustered, we need to sort the relation by the indexed column
          // first.
          if (isClustered) {
            logger.info(
                "Sorting relation {} for clustered index on column {}", tableName, columnName);

            // Get the column index for sorting
            int columnIndex = getColumnIndex(tableName, columnName);
            if (columnIndex == -1) {
              throw new IOException("Column " + columnName + " not found in table " + tableName);
            }

            String relationPath = dataDir + File.separator + tableName;
            String tempSortedPath = tempDir + File.separator + tableName + "_sorted";

            // Read, sort, and rewrite the relation
            try (TupleReader reader = new TupleReader(relationPath)) {
              List<int[]> tuples = reader.readTuples();
              List<int[]> metadata = reader.readMetaData();

              // Sort tuples based on the index column
              sortTuplesOnColumn(tuples, metadata, columnIndex);

              // Write sorted relation back
              try (TupleWriter writer = new TupleWriter(tempSortedPath)) {
                for (int[] tuple : tuples) {
                  writer.writeTuple(tuple);
                }
              }
            }

            // Replace the original file with the sorted one
            Files.move(
                Paths.get(tempSortedPath),
                Paths.get(relationPath),
                StandardCopyOption.REPLACE_EXISTING);

            logger.info("Successfully sorted relation {} for clustered index", tableName);
          }

          // Now that the table is sorted if clustered, build the index using BulkLoader.
          BulkLoader loader = new BulkLoader(indexInfoPath, outputFile);
          loader.buildAndSerialize();

          // Log the index header info for verification
          try (RandomAccessFile raf = new RandomAccessFile(outputFile, "r")) {
            int rootAddr = raf.readInt();
            int numLeaves = raf.readInt();
            int indexOrder = raf.readInt();
            logger.info(
                "Index {}.{} header: RootAddr={}, NumLeaves={}, Order={}",
                tableName,
                columnName,
                rootAddr,
                numLeaves,
                indexOrder);
          }

          logger.info("Successfully built index for {}.{}", tableName, columnName);

        } catch (Exception e) {
          logger.error("Error building index for {}.{}: {}", tableName, columnName, e.getMessage());
          e.printStackTrace();
        }
      } else {
        logger.warn("Skipping invalid index configuration line: {}", line);
      }
    }

    logger.info("Index building completed. Built {} indexes.", totalIndexes);
  }

  /**
   * Sorts tuples based on a specific column while maintaining metadata relationships.
   *
   * @param tuples List of tuples to sort
   * @param metadata Associated metadata for each tuple
   * @param columnIndex Index of the column to sort on
   */
  private static void sortTuplesOnColumn(
      List<int[]> tuples, List<int[]> metadata, int columnIndex) {
    // Create pairs of tuples and their metadata for stable sorting
    List<Pair<int[], int[]>> pairs = new ArrayList<>();
    for (int i = 0; i < tuples.size(); i++) {
      pairs.add(new Pair<>(tuples.get(i), metadata.get(i)));
    }

    // Sort based on the column value while maintaining tuple-metadata relationship
    Collections.sort(
        pairs, (a, b) -> Integer.compare(a.getKey()[columnIndex], b.getKey()[columnIndex]));

    // Update the original lists with sorted data
    for (int i = 0; i < pairs.size(); i++) {
      tuples.set(i, pairs.get(i).getKey());
      metadata.set(i, pairs.get(i).getValue());
    }
  }

  /**
   * Gets the index of a column in a table's schema.
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @return Index of the column, or -1 if not found
   */
  private static int getColumnIndex(String tableName, String columnName) {
    ArrayList<Column> columns = DBCatalog.getInstance().getColumns(tableName);
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getColumnName().equals(columnName)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Verifies the required directory structure exists and is properly configured. Creates necessary
   * directories and cleans output/temp directories.
   */
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

    // Create indexes directory if it doesn't exist
    File indexesDir = new File(dbDir, "indexes");
    if (!indexesDir.exists()) {
      indexesDir.mkdirs();
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

  /** Utility class for maintaining relationships between pairs of objects. */
  private static class Pair<K, V> {
    private final K key;
    private final V value;

    public Pair(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }
  }
}
