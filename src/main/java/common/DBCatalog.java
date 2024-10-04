package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class to contain information about database - names of tables, schema of each table and file
 * where each table is located. Uses singleton pattern.
 *
 * <p>Assumes dbDirectory has a schema.txt file and a /data subdirectory containing one file per
 * relation, named "relname".
 *
 * <p>Call by using DBCatalog.getInstance();
 */
public class DBCatalog {
  private final Logger logger = LogManager.getLogger();

  private final HashMap<String, ArrayList<Column>> tables;
  private static DBCatalog db;

  private String dbDirectory;

  /** Reads schemaFile and populates schema information */
  private DBCatalog() {
    tables = new HashMap<>();
  }

  /**
   * Instance getter for singleton pattern, lazy initialization on first invocation
   *
   * @return unique DB catalog instance
   */
  public static DBCatalog getInstance() {
    if (db == null) {
      db = new DBCatalog();
    }
    return db;
  }

  /**
   * Sets the data directory for the database catalog.
   *
   * @param directory: The input directory.
   */
  public void setDataDirectory(String directory) {
    // this.dbDirectory = directory;

    // now we load schema
    // try (BufferedReader br = new BufferedReader(new FileReader(dbDirectory + "/schema.txt"))) {
    //   String line;
    //   while ((line = br.readLine()) != null) {
    //     String[] parts = line.split(" ");
    //     String tableName = parts[0];
    //     ArrayList<Column> columns = new ArrayList<>();
    //     for (int i = 1; i < parts.length; i++) {
    //       columns.add(new Column(new Table(tableName), parts[i]));
    //     }
    //     tables.put(tableName, columns);
    //   }
    // } catch (IOException e) {
    //   logger.error("Error reading schema file", e);
    // }

    // 
    try {
      dbDirectory = directory;
      BufferedReader br = new BufferedReader(new FileReader(directory + "/schema.txt"));
      String line;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split("\\s");
        String tableName = tokens[0];
        ArrayList<Column> cols = new ArrayList<Column>();
        for (int i = 1; i < tokens.length; i++) {
          cols.add(new Column(new Table(null, tableName), tokens[i]));
        }
        tables.put(tokens[0], cols);
      }
      br.close();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * Gets path to file where a particular table is stored
   *
   * @param tableName table name
   * @return file where table is found on disk
   */
  public File getFileForTable(String tableName) {
    return new File(dbDirectory + "/data/" + tableName);
  }

  /** Gets the schema of a table */
  public ArrayList<Column> getColumns(String tableName) {
    return tables.get(tableName);
  }
}
