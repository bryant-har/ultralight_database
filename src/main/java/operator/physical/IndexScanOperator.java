package operator.physical;

import common.DBCatalog;
import common.Tuple;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;

public class IndexScanOperator extends Operator {
  private static final int PAGE_SIZE = 4096;
  private final String relationName;
  private final String indexFile;
  private final boolean isClustered;
  private final Integer lowKey;
  private final Integer highKey;

  private RandomAccessFile indexRAF;
  private RandomAccessFile dataFileRAF;
  private ByteBuffer buffer;
  private int rootAddress;
  private int currentLeafPage;
  private int currentEntryIndex;
  private List<int[]> currentRids;
  private int currentRidIndex;
  private List<int[]> allTuples;
  private int currentTupleIndex;
  private boolean initialized;
  private final String indexedColumn; // Add this as a class field

  /**
   * Constructs an IndexScanOperator for scanning a relation using a B+ tree
   * index.
   *
   * @param outputSchema The schema of the output tuples
   * @param relationName The name of the relation to scan
   * @param indexFile    Path to the index file
   * @param isClustered  Whether the index is clustered
   * @param lowKey       Lower bound of the range to scan (null for unbounded)
   * @param highKey      Upper bound of the range to scan (null for unbounded)
   */

  // Then modify the constructor to take and store the column name
  public IndexScanOperator(
      ArrayList<Column> outputSchema,
      String relationName,
      String indexFile,
      boolean isClustered,
      Integer lowKey,
      Integer highKey,
      String indexedColumn) {
    super(outputSchema);
    this.relationName = relationName;
    this.indexFile = indexFile;
    this.isClustered = isClustered;
    this.lowKey = lowKey;
    this.highKey = highKey;
    this.indexedColumn = indexedColumn;
    this.buffer = ByteBuffer.allocate(PAGE_SIZE);
    this.initialized = false;
    this.currentRids = new ArrayList<>();
    this.currentRidIndex = 0;
    this.currentTupleIndex = 0;
  }

  /**
   * Initializes the operator by opening necessary files and performing initial B+
   * tree traversal.
   */
  private void initialize() throws IOException {
    if (initialized)
      return;

    // Debug indexing
    System.out.println("Starting index scan with bounds: [" + lowKey + ", " + highKey + "]");

    indexRAF = new RandomAccessFile(indexFile, "r");
    String dataFilePath = DBCatalog.getInstance().getFileForTable(relationName).getAbsolutePath();
    dataFileRAF = new RandomAccessFile(dataFilePath, "r");

    buffer.clear();
    indexRAF.seek(0);
    indexRAF.read(buffer.array());
    rootAddress = buffer.getInt(0);
    System.out.println("Root address from index: " + rootAddress);

    currentLeafPage = findStartLeaf(rootAddress);
    System.out.println("Found starting leaf page: " + currentLeafPage);

    buffer.clear();
    indexRAF.seek((long) currentLeafPage * PAGE_SIZE);
    indexRAF.read(buffer.array());

    currentEntryIndex = 0;

    // Print first few entries in leaf
    int numEntries = buffer.getInt(4);
    System.out.println("Leaf has " + numEntries + " entries");
    for (int i = 0; i < Math.min(3, numEntries); i++) {
      int offset = calculateEntryOffset(i);
      int key = buffer.getInt(offset);
      System.out.println("Entry " + i + " has key: " + key);
    }

    boolean foundValidEntry = false;
    while (!foundValidEntry && currentEntryIndex < buffer.getInt(4)) {
      int entryOffset = calculateEntryOffset(currentEntryIndex);
      int key = buffer.getInt(entryOffset);
      System.out.println("Checking entry with key: " + key + " against lowKey: " + lowKey);
      if (lowKey == null || key >= lowKey) {
        foundValidEntry = true;
        readEntryAtOffset(entryOffset);
      } else {
        currentEntryIndex++;
      }
    }

    initialized = true;
  }

  private boolean readNextEntry() throws IOException {
    if (buffer.getInt(0) != 0) {
      System.out.println("Not a leaf node type: " + buffer.getInt(0));
      return false;
    }

    int numEntries = buffer.getInt(4);
    System.out.println("Current entry: " + currentEntryIndex + " of " + numEntries);
    if (currentEntryIndex >= numEntries) {
      System.out.println("No more entries in current leaf");
      return false;
    }

    int offset = calculateEntryOffset(currentEntryIndex);
    int key = buffer.getInt(offset);
    System.out.println("Reading entry with key: " + key);

    if (highKey != null && key > highKey) {
      System.out.println("Key exceeds highKey " + highKey);
      return false;
    }

    int numRids = buffer.getInt(offset + 4);
    System.out.println("Entry has " + numRids + " RIDs");
    offset += 8;

    currentRids.clear();
    for (int i = 0; i < numRids; i++) {
      int pageId = buffer.getInt(offset + i * 8);
      int tupleId = buffer.getInt(offset + i * 8 + 4);
      currentRids.add(new int[] { pageId, tupleId });
      System.out.println("Added RID: (" + pageId + "," + tupleId + ")");
    }

    currentEntryIndex++;
    currentRidIndex = 0;
    return true;
  }

  private int calculateEntryOffset(int entryIndex) {
    int offset = 8;
    for (int i = 0; i < entryIndex; i++) {
      int numRids = buffer.getInt(offset + 4);
      offset += 8 + (numRids * 8);
    }
    return offset;
  }

  private void readEntryAtOffset(int offset) {
    int key = buffer.getInt(offset);
    int numRids = buffer.getInt(offset + 4);
    offset += 8;

    currentRids.clear();
    for (int i = 0; i < numRids; i++) {
      int pageId = buffer.getInt(offset + i * 8);
      int tupleId = buffer.getInt(offset + i * 8 + 4);
      currentRids.add(new int[] { pageId, tupleId });
      System.out.println("Reading entry key=" + key + ", RID=(" + pageId + "," + tupleId + ")");
    }
    currentRidIndex = 0;
  }

  private int findStartLeaf(int nodeAddress) throws IOException {
    buffer.clear();
    indexRAF.seek((long) nodeAddress * PAGE_SIZE);
    indexRAF.read(buffer.array());

    int nodeType = buffer.getInt(0);

    if (nodeType == 0) {
      return nodeAddress;
    }

    int numKeys = buffer.getInt(4);
    int childPointerOffset = 8 + (numKeys * 4);
    int childPtr = buffer.getInt(childPointerOffset);

    if (lowKey == null) {
      return findStartLeaf(childPtr);
    }

    for (int i = 0; i < numKeys; i++) {
      int key = buffer.getInt(8 + i * 4);
      if (key > lowKey) {
        return findStartLeaf(childPtr);
      }
      childPtr = buffer.getInt(childPointerOffset + (i + 1) * 4);
    }

    return findStartLeaf(childPtr);
  }

  @Override
  public Tuple getNextTuple() {
    try {
      if (!initialized) {
        initialize();
      }

      while (true) {
        if (currentRidIndex < currentRids.size()) {
          int[] rid = currentRids.get(currentRidIndex++);

          if (isClustered) {
            if (currentTupleIndex < allTuples.size()) {
              Tuple tuple = new Tuple(allTuples.get(currentTupleIndex++));
              int key = tuple.getElementAtIndex(getKeyColumnIndex());
              if (key > highKey)
                return null;
              return tuple;
            }
            return null;
          } else {
            Tuple tuple = readTupleFromRID(rid);
            int key = tuple.getElementAtIndex(getKeyColumnIndex());
            if (key > highKey) {
              return null;
            }
            return tuple;
          }
        }

        if (!readNextEntry()) {
          return null;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  private Tuple readTupleFromRID(int[] rid) throws IOException {
    buffer.clear();
    long pageOffset = (long) rid[0] * PAGE_SIZE;
    dataFileRAF.seek(pageOffset);

    int numAttrs = dataFileRAF.readInt();
    int numTuples = dataFileRAF.readInt();

    int tupleSize = numAttrs * 4;
    long tupleOffset = pageOffset + 8 + ((long) rid[1] * tupleSize);

    dataFileRAF.seek(tupleOffset);

    int[] tupleData = new int[numAttrs];
    for (int i = 0; i < numAttrs; i++) {
      tupleData[i] = dataFileRAF.readInt();
    }

    return new Tuple(tupleData);
  }

  private boolean readNextEntry() throws IOException {
    if (buffer.getInt(0) != 0) { // Not a leaf node
      System.out.println("Not a leaf node - type: " + buffer.getInt(0));
      return false;
    }

    int numEntries = buffer.getInt(4);
    System.out.println("Number of entries in leaf: " + numEntries);
    if (currentEntryIndex >= numEntries) {
      System.out.println("No more entries in current leaf");
      return false;
    }

    // Calculate offset to current entry
    int offset = 8;
    for (int i = 0; i < currentEntryIndex; i++) {
      int numRids = buffer.getInt(offset + 4);
      offset += 8 + (numRids * 8); // key + numRids + (rids * 2 ints each)
    }

    int key = buffer.getInt(offset);
    System.out.println("Reading entry with key: " + key);

    // Check if we've passed high key
    if (highKey != null && key > highKey) {
      System.out.println("Key " + key + " exceeds highKey " + highKey);
      return false;
    }

    int numRids = buffer.getInt(offset + 4);
    offset += 8;

    currentRids.clear();
    for (int i = 0; i < numRids; i++) {
      int pageId = buffer.getInt(offset + i * 8);
      int tupleId = buffer.getInt(offset + i * 8 + 4);
      currentRids.add(new int[] { pageId, tupleId });
      System.out.println("Added RID: pageId=" + pageId + ", tupleId=" + tupleId);
    }

    currentEntryIndex++;
    currentRidIndex = 0;
    return true;
  }

  /** Reads a page from the index file into the buffer. */
  private void readPage(int pageNum) throws IOException {
    buffer.clear();
    indexRAF.seek(pageNum * PAGE_SIZE);
    indexRAF.read(buffer.array());
  }

  @Override
  public void reset() {
    try {
      if (indexRAF != null) {
        indexRAF.close();
      }
      if (dataFileRAF != null) {
        dataFileRAF.close();
      }
      initialized = false;
      currentRids.clear();
      currentRidIndex = 0;
      currentTupleIndex = 0;
      if (allTuples != null) {
        allTuples.clear();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private int getKeyColumnIndex() {
    for (int i = 0; i < outputSchema.size(); i++) {
      Column col = outputSchema.get(i);
      if (col.getColumnName().equals(indexedColumn)) {
        return i;
      }
    }
    throw new IllegalStateException("Could not find indexed column in schema");
  }

  @Override
  protected void finalize() throws Throwable {
    reset();
    super.finalize();
  }
}
