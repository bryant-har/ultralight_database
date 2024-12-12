package common;

import file_management.TupleReader;
import java.io.*;
import java.nio.*;
import java.util.*;
import net.sf.jsqlparser.schema.Column;

public class BulkLoader {
  private int d;
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
    this.buffer = ByteBuffer.allocate(PAGE_SIZE);

    try (BufferedReader br = new BufferedReader(new FileReader(indexInfoFilePath))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split("\\s+");
        if (parts.length >= 4) {
          String expectedOutputFile = indexInfoFilePath.substring(0, indexInfoFilePath.lastIndexOf("/"))
              + "/indexes/"
              + parts[0]
              + "."
              + parts[1];
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

    this.keyColumnIndex = getColumnIndex(relationName, indexColumn);
    if (keyColumnIndex == -1) {
      throw new IOException(
          "Could not find column " + indexColumn + " in relation " + relationName);
    }

    this.raf = new RandomAccessFile(outputFileName, "rw");
    nextAddress = 1;
  }

  private int getColumnIndex(String tableName, String columnName) {
    ArrayList<Column> columns = dbCatalog.getColumns(tableName);
    System.out.println("Looking up index for column " + columnName + " in table " + tableName);
    for (int i = 0; i < columns.size(); i++) {
      System.out.println("Column " + i + ": " + columns.get(i).getColumnName());
      if (columns.get(i).getColumnName().equals(columnName)) {
        System.out.println("Found " + columnName + " at index " + i);
        return i;
      }
    }
    return -1;
  }

  public void buildAndSerialize() throws IOException {
    scanRelation(relationName, indexColumn);

    if (dataEntries.isEmpty()) {
      writeHeaderPage(0, 0);
      return;
    }

    nextAddress = 1;
    List<TreeNode> currentLevel = buildLeafNodes();
    int numberOfLeaves = currentLevel.size();
    List<List<TreeNode>> allLevels = new ArrayList<>();
    allLevels.add(currentLevel);

    while (currentLevel.size() > 1) {
      currentLevel = buildIndexNodes(currentLevel);
      allLevels.add(currentLevel);
    }

    int rootAddress = 0;
    for (int i = 0; i < allLevels.size(); i++) {
      List<TreeNode> level = allLevels.get(i);
      for (TreeNode node : level) {
        node.address = nextAddress++;
        serializeNode(node, node.address);
        if (i == allLevels.size() - 1) {
          rootAddress = node.address;
        }
      }
    }

    writeHeaderPage(rootAddress, numberOfLeaves);
  }

  private void validateAndAddEntry(int key, int pageId, int tupleId) {
    try {
      String dataFilePath = DBCatalog.getInstance().getFileForTable(relationName).getAbsolutePath();
      System.out.println("Validating for key=" + key + ", RID=(" + pageId + "," + tupleId + ")");

      try (RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "r")) {
        long offset = (long) pageId * PAGE_SIZE;
        dataFile.seek(offset);
        int numAttrs = dataFile.readInt();
        int numTuples = dataFile.readInt();

        if (tupleId >= numTuples) {
          System.out.println("Invalid RID: tuple " + tupleId + " exceeds tuple count " + numTuples);
          return;
        }

        long tupleOffset = offset + 8 + ((long) tupleId * numAttrs * 4);
        dataFile.seek(tupleOffset + (keyColumnIndex * 4));
        int actualValue = dataFile.readInt();
        System.out.println("Read value=" + actualValue + " at column " + keyColumnIndex);

        if (actualValue == key) {
          int[] rid = new int[] { pageId, tupleId };
          if (!containsRID(currentEntry.rids, rid)) {
            currentEntry.rids.add(rid);
            System.out.println("Added RID (" + pageId + "," + tupleId + ") for key " + key);
          } else {
            System.out.println("RID already exists for key " + key);
          }
        } else {
          System.out.println("Key mismatch: expected " + key + ", found " + actualValue);
        }
      }
    } catch (IOException e) {
      System.out.println("Error validating RID: " + e.getMessage());
      e.printStackTrace();
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

    try (TupleReader reader = new TupleReader(fileName)) {
      List<int[]> tuples = reader.readTuples();
      List<int[]> metaDataForTuples = reader.readMetaData();

      Map<Integer, DataEntry> entries = new TreeMap<>();
      for (int i = 0; i < Math.min(10, tuples.size()); i++) {
        System.out.println("Tuple " + i + " column A value: " + tuples.get(i)[keyColumnIndex]);
        int[] rid = metaDataForTuples.get(i);
        System.out.println("Tuple " + i + " has RID: (" + rid[0] + "," + rid[1] + ")");
      }

      for (int i = 0; i < tuples.size(); i++) {
        int key = tuples.get(i)[keyColumnIndex];
        int[] rid = metaDataForTuples.get(i);
        currentEntry = entries.computeIfAbsent(key, k -> new DataEntry(k, new ArrayList<>()));
        validateAndAddEntry(key, rid[0], rid[1]);
      }

      dataEntries.addAll(entries.values());

      if (isClustered) {
        writeSortedRelation();
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void writeSortedRelation() throws IOException {
    String sortedFileName = DBCatalog.getInstance().getFileForTable(relationName).getAbsolutePath() + "_sorted";

    try (RandomAccessFile dataFile = new RandomAccessFile(
        DBCatalog.getInstance().getFileForTable(relationName).getAbsolutePath(), "r");
        RandomAccessFile sortedFile = new RandomAccessFile(sortedFileName, "rw")) {

      dataFile.seek(0);
      int numAttrs = dataFile.readInt();
      int tupleSize = numAttrs * 4;

      sortedFile.writeInt(numAttrs);
      sortedFile.writeInt(dataEntries.size());

      for (DataEntry entry : dataEntries) {
        for (int[] rid : entry.rids) {
          long tupleOffset = (long) rid[0] * PAGE_SIZE + 8 + ((long) rid[1] * tupleSize);
          dataFile.seek(tupleOffset);
          byte[] tuple = new byte[tupleSize];
          dataFile.read(tuple);
          sortedFile.write(tuple);
        }
      }
    }
  }

  private List<TreeNode> buildLeafNodes() {
    List<TreeNode> leafNodes = new ArrayList<>();
    int totalEntries = dataEntries.size();
    int i = 0;

    while (i < totalEntries) {
      int entriesToAdd;
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

    return leafNodes;
  }

  private List<TreeNode> buildIndexNodes(List<TreeNode> childNodes) {
    List<TreeNode> indexNodes = new ArrayList<>();
    int i = 0;
    int totalChildren = childNodes.size();

    while (i < totalChildren) {
      int remainingChildren = totalChildren - i;
      int numChildren;

      if (remainingChildren > 2 * d + 1 && remainingChildren < 3 * d + 2) {
        numChildren = remainingChildren / 2;
      } else {
        numChildren = Math.min(2 * d + 1, remainingChildren);
      }

      TreeNode indexNode = new TreeNode(false);

      for (int j = 0; j < numChildren; j++) {
        TreeNode child = childNodes.get(i + j);
        indexNode.children.add(child);

        if (j < numChildren - 1) {
          indexNode.keys.add(childNodes.get(i + j + 1).getMinKey());
        }
      }

      indexNodes.add(indexNode);
      i += numChildren;
    }

    return indexNodes;
  }

  private void writeHeaderPage(int rootAddress, int numberOfLeaves) throws IOException {
    raf.seek(0);
    raf.writeInt(rootAddress);
    raf.writeInt(numberOfLeaves);
    raf.writeInt(d);

    for (int i = 3; i < PAGE_SIZE / 4; i++) {
      raf.writeInt(0);
    }
  }

  private void serializeNode(TreeNode node, int address) throws IOException {
    raf.seek((long) address * PAGE_SIZE);
    if (node.isLeaf) {
      raf.writeInt(0);
      raf.writeInt(node.entries.size());
      for (DataEntry entry : node.entries) {
        raf.writeInt(entry.key);
        raf.writeInt(entry.rids.size());
        for (int[] rid : entry.rids) {
          raf.writeInt(rid[0]);
          raf.writeInt(rid[1]);
        }
      }
    } else {
      raf.writeInt(1);
      raf.writeInt(node.keys.size());
      for (int key : node.keys) {
        raf.writeInt(key);
      }
      for (TreeNode child : node.children) {
        raf.writeInt(child.address);
      }
    }

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
