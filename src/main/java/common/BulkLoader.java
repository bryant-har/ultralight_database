package common;

import file_management.TupleReader;
import java.io.*;
import java.nio.*;
import java.util.*;

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

  public BulkLoader(String indexInfoFilePath, String outputFileName) throws IOException {
    List<RelationInfo> relations = parseIndexInfoFile(indexInfoFilePath);

    // we process only one rn
    RelationInfo relation = relations.get(0);
    this.relationName = relation.relationName;
    this.col = relation.indexName;
    this.isClustered = relation.isClustered;
    this.d = relation.d;
    scanRelation(relationName, col);

    this.raf = new RandomAccessFile(outputFileName, "rw");

    dataEntries = new ArrayList<>();
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
    String tableName = "";
    try (TupleReader reader = new TupleReader(tableName)) {

      List<int[]> tuples = reader.readTuples();
      // this contains the references to which pages each tuple is on, data for tuples[1] is @
      // metaDataForTuples[1]
      List<int[]> metaDataForTuples = reader.readMetaData();

      HashMap<Integer, List<int[]>> indexes = new HashMap<>();

      int colIndex = 1;
      // todo, we need to convert this.col to the index of the col, we get this data from scehma
      for (int i = 0; i < tuples.size(); i++) {
        // add to hashmap  (tupe, metadata)
        if (indexes.get(i) != null) {
          indexes.get(i).add(metaDataForTuples.get(i));
        } else {
          indexes.put(tuples.get(i)[colIndex], List.of(metaDataForTuples.get(i)));
        }
      }
      Set<Integer> keys = indexes.keySet();
      // sort by keys
      List<Integer> sortedKeys = new ArrayList<>(keys);
      Collections.sort(sortedKeys);

      for (int i : sortedKeys) {
        DataEntry temp = new DataEntry(i, indexes.get(i));
        dataEntries.add(temp);
      }

    } catch (IOException e) {
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

      // Add the entries to the leaf node, and add the leaf node to the list
      // addEntry method handles adding the key to the node
      TreeNode leafNode = new TreeNode(true);
      for (int j = 0; j < entriesToAdd && i < totalEntries; j++, i++) {
        leafNode.addEntry(dataEntries.get(i));
      }
      leafNodes.add(leafNode);
    }

    return leafNodes;
  }

  private TreeNode buildAndSerialize() throws IOException {
    List<TreeNode> currentLevel = buildLeafNodes();

    while (currentLevel.size() > 1){
      currentLevel = buildIndexNodes(currentLevel);
    }

    return currentLevel.get(0);

  }

  // private List<Integer> serializeNodes(List<TreeNode> nodes) throws IOException {
  //   List<Integer> addresses = new ArrayList<>();

  //   for (TreeNode node : nodes) {
  //     int address = nextAddress;
  //     addresses.add(address);
  //     serializeNode(nodes, address);
  //     nextAddress++;
  //   }

  //   return addresses;
  // }

  // private void serializeNode(TreeNode node, int address) throws IOException {
  //   raf.seek((long) address * PAGE_SIZE);

  //   //leaf noodes
  //   if (node.isLeaf) {
  //     raf.writeInt(0);
  //     raf.writeInt(node.entries.size());

  //     for (DataEntry entry : node.entries) {
  //       raf.writeInt(entry.key);
  //       raf.writeInt(entry.tuple.length);
  //       for (int i = 0; i < entry.tuple.length; i++) {
  //         raf.writeInt(entry.tuple[i]);

  //       }
  //     };
  //         raf.writeInt(rid.tupleId);
  //       }
  //     }
  //   } else {
  //     // index nodes
  //     raf.writeInt(1);
  //     raf.writeInt(node.keys.size());

  //     //write
  //     for (int key : node.keys) {
  //       raf.writeInt(key);
  //     }

  //     // addredsses
  //     for (int childAddress : node.childAddresses) {
  //       raf.writeInt(childAddress);
  //     }
  //   }
  //   // fill 0s
  //   long currentPosition = raf.getFilePointer();
  //   long endOfPage = ((long) (address + 1) * PAGE_SIZE);
  //   while (currentPosition < endOfPage) {
  //     raf.writeByte(0);
  //     currentPosition++;
  //   }}
  // }

  private List<TreeNode> buildIndexNodes(List<TreeNode> childNodes) {
    List<TreeNode> indexNodes = new ArrayList<>();
    int d = childNodes.size();
    int i = 0;

    while (i < d) {

      // should it be 2d -1??
      int nodesToAdd = 2 * d + 1;
      int keysToAdd = 2 * d;
      int remainingChildren = d - i;
      if (keysToAdd > 2 * d && keysToAdd < 3 * d) {
        nodesToAdd = remainingChildren / 2;
        keysToAdd = nodesToAdd - 1;
      }

      TreeNode indexNode = new TreeNode(false);
      for (int j = 0; j < nodesToAdd && i < d; j++) {
        TreeNode child = childNodes.get(i);
        indexNode.children.add(child);

        // Add key if it's not the last child
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

    public TreeNode(boolean isLeaf) {
      this.isLeaf = isLeaf;
      this.keys = new ArrayList<>();
      this.children = new ArrayList<>();
      this.entries = new ArrayList<>();
    }

    public void addEntry(DataEntry entry) {
      if (!isLeaf) {
        throw new IllegalStateException("Cant add entry to non-leaf nodes");
      }
      entries.add(entry);
      keys.add(entry.key);
    }
  }
}
