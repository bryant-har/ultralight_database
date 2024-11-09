package common;

import file_management.TupleReader;
import java.io.*;
import java.nio.*;
import java.util.*;
import net.sf.jsqlparser.schema.Column;

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
  private int PAGE_SIZE = 4096;
  private DBCatalog dbCatalog;

  public BulkLoader(String indexInfoFilePath, String outputFileName) throws IOException {
    List<RelationInfo> relations = parseIndexInfoFile(indexInfoFilePath);

    // TODO: index all relations in the file, we process only one rn
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

  public void scanRelation(String relationName, String col) {
    String fileName = "src/test/resources/samples/input/db_p2/data/" + relationName;
    System.out.println("Reading data from: " + fileName);

    try (TupleReader reader = new TupleReader(fileName)) {
      List<int[]> tuples = reader.readTuples();
      List<int[]> metaDataForTuples = reader.readMetaData();

      System.out.println("Number of tuples read: " + tuples.size());
      if (!tuples.isEmpty()) {
        System.out.println("First tuple: " + Arrays.toString(tuples.get(0)));
      }

      HashMap<Integer, List<int[]>> indexes = new HashMap<>();

      int colIndex = getColumnIndex(relationName, col);
      for (int i = 0; i < tuples.size(); i++) {
        int key = tuples.get(i)[colIndex];
        List<int[]> ridList = indexes.getOrDefault(key, new ArrayList<>());
        ridList.add(metaDataForTuples.get(i));
        indexes.put(key, ridList);
      }

      Set<Integer> keys = indexes.keySet();
      List<Integer> sortedKeys = new ArrayList<>(keys);
      Collections.sort(sortedKeys);

      for (int i : sortedKeys) {
        DataEntry temp = new DataEntry(i, indexes.get(i));
        dataEntries.add(temp);
      }

      // checking for clustering
      if (isClustered) {
        dataEntries.sort(Comparator.comparingInt(entry -> entry.key));
        writeSortedRelation(relationName); // save srted relation
      }

    } catch (IOException e) {
      System.err.println("Error reading file: " + fileName);
      e.printStackTrace();
    }
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

  private void writeSortedRelation(String relationName) {
    String sortedFileName =
        "src/test/resources/samples/input/db_p2/data/" + relationName + "_sorted";
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

  private List<TreeNode> buildLeafNodes() {
    List<TreeNode> leafNodes = new ArrayList<>();

    int totalEntries = dataEntries.size();
    int i = 0;

    while (i < totalEntries) {
      int entriesToAdd = 2 * d;

      // special case of last two nodes if needed
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
        // this is the root
        rootAddress = currentLevel.get(0).address;
        break;
      }
      // otherwise recursively serialize the nodes
      nextLevel = buildIndexNodes(currentLevel);
      currentLevel = nextLevel;
    }

    writeHeaderPage(rootAddress, numberOfLeaves);
  }

  private void writeHeaderPage(int rootAddress, int numberOfLeaves) throws IOException {
    raf.seek(0);
    raf.writeInt(rootAddress);
    System.out.println("Header Page - RootAddress: " + rootAddress);
    raf.writeInt(numberOfLeaves);
    System.out.println("Header Page - Number of Leaves: " + numberOfLeaves);
    raf.writeInt(d);
    System.out.println("Header Page - d: " + d);
    // fill the rest with zeros
    for (int i = 3; i < PAGE_SIZE / 4; i++) {
      raf.writeInt(0);
    }
  }

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
      // this is an index node
      raf.writeInt(1); // index node flag
      raf.writeInt(node.keys.size());
      for (int key : node.keys) {
        raf.writeInt(key);
      }
      for (TreeNode child : node.children) {
        raf.writeInt(child.address);
      }
    }

    // Fill the rest of the page with zeros
    long currentPosition = raf.getFilePointer();
    long end = ((long) (address + 2) * PAGE_SIZE);
    while (currentPosition < end) {
      raf.writeByte(0);
      currentPosition++;
    }
  }

  private List<TreeNode> buildIndexNodes(List<TreeNode> childNodes) {
    List<TreeNode> indexNodes = new ArrayList<>();
    int totalChildren = childNodes.size();
    int i = 0;

    while (i < totalChildren) {
      int remainingChildren = totalChildren - i;
      int nodesToAdd;
      int keysToAdd;
      if (remainingChildren > 2 * d + 1 && remainingChildren < 3 * d + 2) {
        // case for last two nodes
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
        throw new IllegalStateException("Can't add entry to non-leaf nodes");
      }
      entries.add(entry);
      keys.add(entry.key);
    }
  }
}
