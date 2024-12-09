package common;

import file_management.TupleReader;
import java.io.*;
import java.nio.*;
import java.util.*;
import net.sf.jsqlparser.schema.Column;

public class BulkLoader {
  private int d; // order of the tree
  private String relationName;
  private String indexColumn;
  private boolean isClustered;
  private List<DataEntry> dataEntries;
  private int nextAddress;
  private String outputFileName;
  private RandomAccessFile raf;
  private ByteBuffer buffer;
  private static final int PAGE_SIZE = 4096;
  private DBCatalog dbCatalog;
  private int keyColumnIndex;
  private DataEntry currentEntry;

  public BulkLoader(String indexInfoFilePath, String outputFileName) throws IOException {
    this.outputFileName = outputFileName;
    this.dbCatalog = DBCatalog.getInstance();
    this.dataEntries = new ArrayList<>();

    // Parse index info to find the info for this specific index
    try (BufferedReader br = new BufferedReader(new FileReader(indexInfoFilePath))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split("\\s+");
        if (parts.length >= 4) {
          // Check if this line corresponds to our target index
          String expectedOutputFile = indexInfoFilePath.substring(0, indexInfoFilePath.lastIndexOf("/")) +
              "/indexes/" + parts[0] + "." + parts[1];
          if (expectedOutputFile.equals(outputFileName)) {
            this.relationName = parts[0];
            this.indexColumn = parts[1];
            this.isClustered = parts[2].equals("1");
            this.d = Integer.parseInt(parts[3]);
            break;
          }
        }
      }
    }

    if (relationName == null) {
      throw new IOException("Could not find index configuration for " + outputFileName);
    }

    // Find the column index for the indexed column
    this.keyColumnIndex = getColumnIndex(relationName, indexColumn);
    if (keyColumnIndex == -1) {
      throw new IOException("Could not find column " + indexColumn + " in relation " + relationName);
    }

    this.raf = new RandomAccessFile(outputFileName, "rw");
    nextAddress = 1; // Start at 1 since page 0 is header
  }

  private int getColumnIndex(String tableName, String columnName) {
    ArrayList<Column> columns = dbCatalog.getColumns(tableName);
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getColumnName().equals(columnName)) {
        return i;
      }
    }
    return -1;
  }

  public void buildAndSerialize() throws IOException {
    // First scan the relation to build data entries
    scanRelation(relationName, indexColumn);

    // Start address counting from 1 (header is at 0)
    nextAddress = 1;

    // Build the tree starting from leaf nodes
    List<TreeNode> currentLevel = buildLeafNodes();
    int numberOfLeaves = currentLevel.size();

    // Keep track of levels for final root address calculation
    List<List<TreeNode>> allLevels = new ArrayList<>();
    allLevels.add(currentLevel);

    // Build index nodes until we reach the root
    while (currentLevel.size() > 1) {
      // Build next level
      currentLevel = buildIndexNodes(currentLevel);
      allLevels.add(currentLevel);
    }

    // Now serialize all nodes level by level, starting with leaves
    int rootAddress = 0;
    for (int i = 0; i < allLevels.size(); i++) {
      List<TreeNode> level = allLevels.get(i);
      for (TreeNode node : level) {
        node.address = nextAddress++;
        serializeNode(node, node.address);

        // If this is the root level (last level), save its address
        if (i == allLevels.size() - 1) {
          rootAddress = node.address;
        }
      }
    }

    // Write the header page
    writeHeaderPage(rootAddress, numberOfLeaves);

    // Verify the root address
    System.out.println("Built tree with root address: " + rootAddress +
        ", number of leaves: " + numberOfLeaves +
        ", order: " + d);
  }

  private void validateAndAddEntry(int key, int pageId, int tupleId) {
    try {
      // Get the data file
      String dataFilePath = DBCatalog.getInstance().getFileForTable(relationName).getAbsolutePath();
      try (RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "r")) {
        // Read page header
        long offset = (long) pageId * PAGE_SIZE;
        dataFile.seek(offset);
        int numAttrs = dataFile.readInt();
        int numTuples = dataFile.readInt();

        // Validate tuple ID
        if (tupleId >= numTuples) {
          System.err.println("Invalid RID: tuple " + tupleId + " exceeds page tuple count " + numTuples);
          return;
        }

        // Read the actual value at the key column
        long tupleOffset = offset + 8 + ((long) tupleId * numAttrs * 4);
        dataFile.seek(tupleOffset + (keyColumnIndex * 4));
        int actualValue = dataFile.readInt();

        // Verify the key matches
        if (actualValue == key) {
          int[] rid = new int[] { pageId, tupleId };
          if (!containsRID(currentEntry.rids, rid)) {
            currentEntry.rids.add(rid);
            System.out.println("Added valid RID (" + pageId + "," + tupleId + ") for key " + key);
          }
        } else {
          System.err.println("Key mismatch for RID (" + pageId + "," + tupleId +
              "): expected " + key + " but found " + actualValue);
        }
      }
    } catch (IOException e) {
      System.err.println("Error validating RID: " + e.getMessage());
    }
  }

  private boolean containsRID(List<int[]> rids, int[] rid) {
    for (int[] existingRid : rids) {
      if (existingRid[0] == rid[0] && existingRid[1] == rid[1]) {
        return true;
      }
    }
    return false;
  }

  public void scanRelation(String relationName, String col) {
    String fileName = DBCatalog.getInstance().getFileForTable(relationName).getAbsolutePath();
    System.out.println("Reading data from: " + fileName);

    try (TupleReader reader = new TupleReader(fileName)) {
      List<int[]> tuples = reader.readTuples();
      List<int[]> metaDataForTuples = reader.readMetaData();

      System.out.println("Number of tuples read: " + tuples.size());
      if (!tuples.isEmpty()) {
        System.out.println("First tuple: " + Arrays.toString(tuples.get(0)));
      }

      // Build index entries grouped by key
      Map<Integer, DataEntry> entries = new TreeMap<>(); // Using TreeMap for sorted keys

      for (int i = 0; i < tuples.size(); i++) {
        int key = tuples.get(i)[keyColumnIndex];
        int[] rid = metaDataForTuples.get(i);

        // Get or create DataEntry for this key
        currentEntry = entries.computeIfAbsent(key, k -> new DataEntry(k, new ArrayList<>()));

        // Validate and add the RID
        validateAndAddEntry(key, rid[0], rid[1]);
      }

      // Add all entries to the list
      dataEntries.addAll(entries.values());

      // For clustered indexes, sort and write relation
      if (isClustered) {
        writeSortedRelation();
      }

    } catch (IOException e) {
      System.err.println("Error reading file: " + fileName);
      e.printStackTrace();
    }
  }

  private void writeSortedRelation() throws IOException {
    String sortedFileName = DBCatalog.getInstance().getFileForTable(relationName).getAbsolutePath() + "_sorted";
    System.out.println("Writing sorted relation to: " + sortedFileName);

    try (RandomAccessFile dataFile = new RandomAccessFile(
        DBCatalog.getInstance().getFileForTable(relationName).getAbsolutePath(), "r");
        RandomAccessFile sortedFile = new RandomAccessFile(sortedFileName, "rw")) {

      // Get schema info
      dataFile.seek(0);
      int numAttrs = dataFile.readInt();
      int tupleSize = numAttrs * 4;

      // Write header for sorted file
      sortedFile.writeInt(numAttrs);
      sortedFile.writeInt(dataEntries.size()); // Total number of tuples

      // Write tuples in sorted order
      for (DataEntry entry : dataEntries) {
        for (int[] rid : entry.rids) {
          // Read tuple from original file
          long tupleOffset = (long) rid[0] * PAGE_SIZE + 8 + ((long) rid[1] * tupleSize);
          dataFile.seek(tupleOffset);

          // Copy tuple to sorted file
          byte[] tuple = new byte[tupleSize];
          dataFile.read(tuple);
          sortedFile.write(tuple);
        }
      }

      System.out.println("Successfully wrote sorted relation with " + dataEntries.size() + " entries");
    }
  }

  private List<TreeNode> buildLeafNodes() {
    List<TreeNode> leafNodes = new ArrayList<>();

    int totalEntries = dataEntries.size();
    int i = 0;

    while (i < totalEntries) {
      int entriesToAdd;

      // Handle special case for last two nodes
      int remainingEntries = totalEntries - i;
      if (remainingEntries > 2 * d && remainingEntries < 3 * d) {
        entriesToAdd = remainingEntries / 2;
      } else {
        entriesToAdd = Math.min(2 * d, remainingEntries);
      }

      TreeNode leafNode = new TreeNode(true);
      for (int j = 0; j < entriesToAdd && i < totalEntries; j++, i++) {
        leafNode.addEntry(dataEntries.get(i));
      }
      leafNodes.add(leafNode);
    }

    System.out.println("Built " + leafNodes.size() + " leaf nodes");
    return leafNodes;
  }

  private List<TreeNode> buildIndexNodes(List<TreeNode> childNodes) {
    List<TreeNode> indexNodes = new ArrayList<>();
    int i = 0;
    int totalChildren = childNodes.size();

    while (i < totalChildren) {
      int remainingChildren = totalChildren - i;
      int numChildren;

      // Handle special case for last two nodes
      if (remainingChildren > 2 * d + 1 && remainingChildren < 3 * d + 2) {
        numChildren = remainingChildren / 2;
      } else {
        numChildren = Math.min(2 * d + 1, remainingChildren);
      }

      TreeNode indexNode = new TreeNode(false);

      // Add children and their keys
      for (int j = 0; j < numChildren; j++) {
        TreeNode child = childNodes.get(i + j);
        indexNode.children.add(child);

        if (j < numChildren - 1) {
          // Use lowest key in next child's subtree
          indexNode.keys.add(childNodes.get(i + j + 1).getMinKey());
        }
      }

      indexNodes.add(indexNode);
      i += numChildren;
    }

    System.out.println("Built " + indexNodes.size() + " index nodes");
    return indexNodes;
  }

  private void writeHeaderPage(int rootAddress, int numberOfLeaves) throws IOException {
    raf.seek(0);
    raf.writeInt(rootAddress);
    raf.writeInt(numberOfLeaves);
    raf.writeInt(d);

    // Fill rest of page with zeros
    for (int i = 3; i < PAGE_SIZE / 4; i++) {
      raf.writeInt(0);
    }
    System.out.println("Wrote header page: root=" + rootAddress + ", leaves=" + numberOfLeaves + ", order=" + d);
  }

  private void serializeNode(TreeNode node, int address) throws IOException {
    raf.seek((long) address * PAGE_SIZE);
    if (node.isLeaf) {
      // Write leaf node
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
      // Write index node
      raf.writeInt(1); // index node flag
      raf.writeInt(node.keys.size());
      for (int key : node.keys) {
        raf.writeInt(key);
      }
      for (TreeNode child : node.children) {
        raf.writeInt(child.address);
      }
    }

    // Fill remaining space with zeros
    long currentPosition = raf.getFilePointer();
    long endPosition = (long) (address + 1) * PAGE_SIZE;
    while (currentPosition < endPosition) {
      raf.writeByte(0);
      currentPosition++;
    }
  }

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

    public void addEntry(DataEntry entry) {
      if (!isLeaf) {
        throw new IllegalStateException("Cannot add entry to non-leaf node");
      }
      entries.add(entry);
      keys.add(entry.key);
    }

    public int getMinKey() {
      if (isLeaf) {
        return entries.get(0).key;
      } else {
        return keys.get(0);
      }
    }
  }
}