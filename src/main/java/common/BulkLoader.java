package common;

import file_management.TupleReader;
import java.io.*;
import java.nio.*;
import java.util.*;
import net.sf.jsqlparser.schema.Column;

/**
 * BulkLoader class implements functionality to build B+ tree indexes for
 * database relations. This
 * class handles both clustered and unclustered indexes, following the bulk
 * loading algorithm which
 * constructs the tree bottom-up for optimal space utilization.
 *
 * <p>
 * The tree has an order 'd' where: - Leaf nodes must have between d and 2d data
 * entries - Index
 * nodes must have between d and 2d keys, and d+1 to 2d+1 child pointers
 *
 * <p>
 * The implementation uses Alternative (3) format where leaf nodes contain data
 * entries of the
 * form <key, list of RIDs> where each RID is a (pageId, tupleId) pair.
 */
public class BulkLoader {
  private int d; // order of the tree
  private String relationName;
  private boolean isClustered;
  private List<DataEntry> dataEntries;
  private int nextAddress;
  private String outputFileName;
  private String col;
  private RandomAccessFile raf;
  private ByteBuffer buffer;
  private static final int PAGE_SIZE = 4096;
  private DBCatalog dbCatalog;

  /**
   * Constructs a BulkLoader to build a B+ tree index. Reads index configuration
   * from the provided
   * file and initializes the bulk loading process.
   *
   * @param indexInfoFilePath Path to the index configuration file
   * @param outputFileName    Path where the serialized B+ tree will be written
   * @throws IOException If there are errors reading the index info file
   */
  public BulkLoader(String indexInfoFilePath, String outputFileName) throws IOException {
    List<RelationInfo> relations = parseIndexInfoFile(indexInfoFilePath);

    // Currently processes only the first relation in the file
    RelationInfo relation = relations.get(0);
    this.relationName = relation.relationName;
    this.col = relation.indexName;
    this.isClustered = relation.isClustered;
    this.d = relation.d;
    dataEntries = new ArrayList<>();
    this.dbCatalog = DBCatalog.getInstance();

    scanRelation(relationName, col);

    this.raf = new RandomAccessFile(outputFileName, "rw");
    nextAddress = 0;
  }

  /**
   * Parses the index information file to extract configuration for each index.
   * File format:
   * relation_name index_name clustered(0/1) order
   *
   * @param indexInfoFilePath Path to the index configuration file
   * @return List of RelationInfo objects containing parsed configurations
   * @throws IOException If there are errors reading the file
   */
  private List<RelationInfo> parseIndexInfoFile(String indexInfoFilePath) throws IOException {
    List<RelationInfo> relations = new ArrayList<>();

    try (BufferedReader br = new BufferedReader(new FileReader(indexInfoFilePath))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split(" ");
        if (parts.length >= 4) {
          String relationName = parts[0];
          String indexName = parts[1];
          boolean isClustered = parts[2].equals("1");
          int d = Integer.parseInt(parts[3]);

          RelationInfo relationInfo = new RelationInfo(relationName, indexName, isClustered, d);
          relations.add(relationInfo);
        }
      }
    }

    return relations;
  }

  /** Inner class to hold index configuration information for a relation. */
  private static class RelationInfo {
    String relationName;
    String indexName;
    boolean isClustered;
    int d;

    public RelationInfo(String relationName, String indexName, boolean isClustered, int d) {
      this.relationName = relationName;
      this.indexName = indexName;
      this.isClustered = isClustered;
      this.d = d;
    }
  }

  /**
   * Scans the relation file and builds data entries for the index. For clustered
   * indexes, also
   * sorts the data entries by key.
   *
   * @param relationName Name of the relation to scan
   * @param col          Name of the column to index
   */
  public void scanRelation(String relationName, String col) {
    // Get file path from DBCatalog
    File tableFile = DBCatalog.getInstance().getFileForTable(relationName);
    String fileName = tableFile.getAbsolutePath();

    System.out.println("Reading data from: " + fileName);

    try (TupleReader reader = new TupleReader(fileName)) {
      List<int[]> tuples = reader.readTuples();
      List<int[]> metaDataForTuples = reader.readMetaData();

      System.out.println("Number of tuples read: " + tuples.size());
      if (!tuples.isEmpty()) {
        System.out.println("First tuple: " + Arrays.toString(tuples.get(0)));
      }

      // Build index entries grouped by key
      HashMap<Integer, List<int[]>> indexes = new HashMap<>();
      int colIndex = getColumnIndex(relationName, col);
      for (int i = 0; i < tuples.size(); i++) {
        int key = tuples.get(i)[colIndex];
        List<int[]> ridList = indexes.getOrDefault(key, new ArrayList<>());
        ridList.add(metaDataForTuples.get(i));
        indexes.put(key, ridList);
      }

      // Create sorted data entries
      Set<Integer> keys = indexes.keySet();
      List<Integer> sortedKeys = new ArrayList<>(keys);
      Collections.sort(sortedKeys);

      for (int i : sortedKeys) {
        DataEntry temp = new DataEntry(i, indexes.get(i));
        dataEntries.add(temp);
      }

      // For clustered indexes, sort entries and write sorted relation
      if (isClustered) {
        dataEntries.sort(Comparator.comparingInt(entry -> entry.key));
        writeSortedRelation(relationName);
      }

    } catch (IOException e) {
      System.err.println("Error reading file: " + fileName);
      e.printStackTrace();
    }
  }

  /**
   * Gets the index of a column in a table's schema.
   *
   * @param tableName  Name of the table
   * @param columnName Name of the column
   * @return Index of the column in the schema, or -1 if not found
   */
  private int getColumnIndex(String tableName, String columnName) {
    ArrayList<Column> columns = dbCatalog.getColumns(tableName);
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getColumnName().equals(columnName)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Writes a sorted version of the relation for clustered indexes.
   *
   * @param relationName Name of the relation to write
   */
  private void writeSortedRelation(String relationName) {
    String sortedFileName = "src/test/resources/samples/input/db_p2/data/" + relationName + "_sorted";
    try (RandomAccessFile sortedFile = new RandomAccessFile(sortedFileName, "rw")) {
      for (DataEntry entry : dataEntries) {
        for (int[] rid : entry.rids) {
          for (int value : rid) {
            sortedFile.writeInt(value);
          }
        }
      }
      System.out.println("Sorted relation written: " + sortedFileName);
    } catch (IOException e) {
      System.err.println("Error writing sorted relation: " + sortedFileName);
      e.printStackTrace();
    }
  }

  /**
   * Builds the leaf nodes of the B+ tree according to the bulk loading algorithm.
   * Each leaf node
   * gets 2d entries except possibly the last two nodes.
   *
   * @return List of constructed leaf nodes
   */
  private List<TreeNode> buildLeafNodes() {
    List<TreeNode> leafNodes = new ArrayList<>();

    int totalEntries = dataEntries.size();
    int i = 0;

    while (i < totalEntries) {
      int entriesToAdd = 2 * d;

      // Handle special case for last two nodes
      int k = totalEntries - i;
      if (k > 2 * d && k < 3 * d) {
        entriesToAdd = k / 2;
      }

      TreeNode leafNode = new TreeNode(true);
      for (int j = 0; j < entriesToAdd && i < totalEntries; j++, i++) {
        leafNode.addEntry(dataEntries.get(i));
        System.out.println("Adding entry: " + dataEntries.get(i).key);
        for (int[] rid : dataEntries.get(i).rids) {
          System.out.println("RID: " + rid[0] + ", " + rid[1]);
        }
      }
      leafNodes.add(leafNode);
    }

    return leafNodes;
  }

  /**
   * Builds and serializes the complete B+ tree. This includes building leaf
   * nodes, index nodes, and
   * writing the header page.
   *
   * @throws IOException If there are errors writing to the output file
   */
  public void buildAndSerialize() throws IOException {
    List<TreeNode> currentLevel = buildLeafNodes();
    System.out.println("Address of leaf nodes: " + currentLevel.get(0).address);
    int currentAddress = 0;
    int numberOfLeaves = currentLevel.size();
    int rootAddress = 0;

    while (!currentLevel.isEmpty()) {
      System.out.println("Next Level of the Tree");

      List<TreeNode> nextLevel = new ArrayList<>();
      for (TreeNode node : currentLevel) {
        serializeNode(node, currentAddress);
        node.address = currentAddress;
        currentAddress++;
      }
      if (currentLevel.size() == 1) {
        rootAddress = currentLevel.get(0).address;
        break;
      }
      nextLevel = buildIndexNodes(currentLevel);
      currentLevel = nextLevel;
    }

    writeHeaderPage(rootAddress, numberOfLeaves);
  }

  /**
   * Writes the header page of the B+ tree file. Contains root address, number of
   * leaves, and tree
   * order.
   *
   * @param rootAddress    Address of the root node
   * @param numberOfLeaves Total number of leaf nodes
   * @throws IOException If there are errors writing to the file
   */
  private void writeHeaderPage(int rootAddress, int numberOfLeaves) throws IOException {
    raf.seek(0);
    raf.writeInt(rootAddress);
    System.out.println("Header Page - RootAddress: " + rootAddress);
    raf.writeInt(numberOfLeaves);
    System.out.println("Header Page - Number of Leaves: " + numberOfLeaves);
    raf.writeInt(d);
    System.out.println("Header Page - d: " + d);

    // Fill rest of page with zeros
    for (int i = 3; i < PAGE_SIZE / 4; i++) {
      raf.writeInt(0);
    }
  }

  /**
   * Serializes a node to the output file. Handles both leaf and index nodes with
   * appropriate
   * formatting.
   *
   * @param node    The node to serialize
   * @param address The address where the node should be written
   * @throws IOException If there are errors writing to the file
   */
  private void serializeNode(TreeNode node, int address) throws IOException {
    raf.seek((address + 1) * PAGE_SIZE);
    if (node.isLeaf) {
      raf.writeInt(0); // leaf node flag
      raf.writeInt(node.entries.size());
      for (DataEntry entry : node.entries) {
        raf.writeInt(entry.key);
        raf.writeInt(entry.rids.size());
        for (int[] rid : entry.rids) {
          raf.writeInt(rid[0]); // pageId
          raf.writeInt(rid[1]); // tupleId
        }
      }
    } else {
      raf.writeInt(1); // index node flag
      raf.writeInt(node.keys.size());
      for (int key : node.keys) {
        raf.writeInt(key);
      }
      for (TreeNode child : node.children) {
        raf.writeInt(child.address);
      }
    }

    // Fill remaining page space with zeros
    long currentPosition = raf.getFilePointer();
    long end = ((long) (address + 2) * PAGE_SIZE);
    while (currentPosition < end) {
      raf.writeByte(0);
      currentPosition++;
    }
  }

  /**
   * Builds the index nodes layer of the B+ tree. Each index node gets 2d+1
   * children and 2d keys
   * except possibly the last two nodes.
   *
   * @param childNodes List of child nodes to build index nodes from
   * @return List of constructed index nodes
   */
  private List<TreeNode> buildIndexNodes(List<TreeNode> childNodes) {
    List<TreeNode> indexNodes = new ArrayList<>();
    int totalChildren = childNodes.size();
    int i = 0;

    while (i < totalChildren) {
      int remainingChildren = totalChildren - i;
      int nodesToAdd;
      int keysToAdd;
      if (remainingChildren > 2 * d + 1 && remainingChildren < 3 * d + 2) {
        nodesToAdd = remainingChildren / 2;
        keysToAdd = nodesToAdd - 1;
      } else {
        nodesToAdd = 2 * d + 1;
        keysToAdd = 2 * d;
      }

      TreeNode indexNode = new TreeNode(false);

      for (int j = 0; j < nodesToAdd && i < totalChildren; j++) {
        TreeNode child = childNodes.get(i);
        indexNode.children.add(child);

        if (j < keysToAdd) {
          indexNode.keys.add(child.keys.get(0));
        }

        i++;
      }

      indexNodes.add(indexNode);
    }

    return indexNodes;
  }

  /**
   * Represents a data entry in a leaf node of the B+ tree. Contains a key and its
   * associated list
   * of RIDs.
   */
  public class DataEntry implements Comparable<DataEntry> {
    int key;
    List<int[]> rids;

    public DataEntry(int key, List<int[]> rids) {
      this.key = key;
      this.rids = rids;
    }

    @Override
    public int compareTo(DataEntry other) {
      return Integer.compare(this.key, other.key);
    }
  }

  /**
   * Represents a node in the B+ tree. Can be either a leaf node containing data
   * entries or an index
   * node containing keys and child pointers.
   */
  public class TreeNode {
    private boolean isLeaf;
    private List<Integer> keys;
    private List<TreeNode> children;
    private List<DataEntry> entries;
    int address;

    public TreeNode(boolean isLeaf) {
      this.isLeaf = isLeaf;
      this.keys = new ArrayList<>();
      this.children = new ArrayList<>();
      this.entries = new ArrayList<>();
      this.address = -1;
    }

    /**
     * Adds a data entry to a leaf node.
     *
     * @param entry The data entry to add
     * @throws IllegalStateException if attempting to add to a non-leaf node
     */
    public void addEntry(DataEntry entry) {
      if (!isLeaf) {
        throw new IllegalStateException("Can't add entry to non-leaf nodes");
      }
      entries.add(entry);
      keys.add(entry.key);
    }

    /**
     * Returns whether this node is a leaf node.
     *
     * @return true if this is a leaf node, false otherwise
     */
    public boolean isLeaf() {
      return isLeaf;
    }

    /**
     * Returns the list of keys in this node.
     *
     * @return List of keys
     */
    public List<Integer> getKeys() {
      return keys;
    }

    /**
     * Returns the list of child nodes.
     *
     * @return List of child nodes
     */
    public List<TreeNode> getChildren() {
      return children;
    }

    /**
     * Returns the list of data entries (only valid for leaf nodes).
     *
     * @return List of data entries
     */
    public List<DataEntry> getEntries() {
      return entries;
    }

    /**
     * Returns the address of this node in the B+ tree file.
     *
     * @return The node's address
     */
    public int getAddress() {
      return address;
    }
  }
}
