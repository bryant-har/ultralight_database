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
  private List<int[]> allTuples; // For clustered index
  private int currentTupleIndex; // For clustered index
  private boolean initialized;
  private final String indexedColumn; // Add this as a class field

  /**
   * Constructs an IndexScanOperator for scanning a relation using a B+ tree index.
   *
   * @param outputSchema The schema of the output tuples
   * @param relationName The name of the relation to scan
   * @param indexFile Path to the index file
   * @param isClustered Whether the index is clustered
   * @param lowKey Lower bound of the range to scan (null for unbounded)
   * @param highKey Upper bound of the range to scan (null for unbounded)
   */

  // Then modify the constructor to take and store the column name
  public IndexScanOperator(
      ArrayList<Column> outputSchema,
      String relationName,
      String indexFile,
      boolean isClustered,
      Integer lowKey,
      Integer highKey,
      String indexedColumn) { // Add this parameter
    super(outputSchema);
    this.relationName = relationName;
    this.indexFile = indexFile;
    this.isClustered = isClustered;
    this.lowKey = lowKey;
    this.highKey = highKey;
    this.indexedColumn = indexedColumn; // Store it
    this.buffer = ByteBuffer.allocate(PAGE_SIZE);
    this.initialized = false;
    this.currentRids = new ArrayList<>();
    this.currentRidIndex = 0;
    this.currentTupleIndex = 0;
  }

  /**
   * Initializes the operator by opening necessary files and performing initial B+ tree traversal.
   */
  private void initialize() throws IOException {
    if (initialized) return;

    // Open index and data files
    indexRAF = new RandomAccessFile(indexFile, "r");
    String dataFilePath = DBCatalog.getInstance().getFileForTable(relationName).getAbsolutePath();
    dataFileRAF = new RandomAccessFile(dataFilePath, "r");

    // Read root address from header page
    buffer.clear();
    indexRAF.seek(0);
    indexRAF.read(buffer.array());
    rootAddress = buffer.getInt(0);

    // Find and read first valid leaf node
    currentLeafPage = findStartLeaf(rootAddress);
    System.out.println("Found starting leaf page: " + currentLeafPage);
    buffer.clear();
    indexRAF.seek((long) currentLeafPage * PAGE_SIZE);
    indexRAF.read(buffer.array());

    currentEntryIndex = 0;

    // Skip entries until we find one >= lowKey
    boolean foundValidEntry = false;
    while (!foundValidEntry && currentEntryIndex < buffer.getInt(4)) {
      int entryOffset = calculateEntryOffset(currentEntryIndex);
      int key = buffer.getInt(entryOffset);
      System.out.println("Checking entry with key: " + key);
      if (lowKey == null || key >= lowKey) {
        foundValidEntry = true;
        readEntryAtOffset(entryOffset);
      } else {
        currentEntryIndex++;
      }
    }

    initialized = true;
  }

  private int calculateEntryOffset(int entryIndex) {
    int offset = 8; // Skip node type and entry count
    for (int i = 0; i < entryIndex; i++) {
      int numRids = buffer.getInt(offset + 4);
      offset += 8 + (numRids * 8); // Skip key, RID count, and RIDs
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
      currentRids.add(new int[] {pageId, tupleId});
      System.out.println("Reading entry key=" + key + ", RID=(" + pageId + "," + tupleId + ")");
    }
    currentRidIndex = 0;
  }

  private int findStartLeaf(int nodeAddress) throws IOException {
    buffer.clear();
    indexRAF.seek((long) nodeAddress * PAGE_SIZE);
    indexRAF.read(buffer.array());

    int nodeType = buffer.getInt(0);
    System.out.println("Examining node at " + nodeAddress + ", type=" + nodeType);

    if (nodeType == 0) { // Leaf node
      return nodeAddress;
    }

    // Index node - find child containing lowKey
    int numKeys = buffer.getInt(4);
    System.out.println("Index node has " + numKeys + " keys");

    int childPointerOffset = 8 + (numKeys * 4); // Skip keys
    int childPtr = buffer.getInt(childPointerOffset); // First child pointer

    // If no lowKey, use leftmost path
    if (lowKey == null) {
      return findStartLeaf(childPtr);
    }

    // Find appropriate child pointer based on lowKey
    for (int i = 0; i < numKeys; i++) {
      int key = buffer.getInt(8 + i * 4);
      System.out.println("Checking key " + key + " against lowKey " + lowKey);
      if (key > lowKey) {
        return findStartLeaf(childPtr);
      }
      childPtr = buffer.getInt(childPointerOffset + (i + 1) * 4);
    }

    // Use rightmost child if lowKey is greater than all keys
    return findStartLeaf(childPtr);
  }

  @Override
  public Tuple getNextTuple() {
    try {
      if (!initialized) {
        initialize();
        System.out.println("Index scan initialized");
        System.out.println("Current leaf page: " + currentLeafPage);
      }

      while (true) {
        if (currentRidIndex < currentRids.size()) {
          int[] rid = currentRids.get(currentRidIndex++);
          System.out.println("Reading tuple with RID: pageId=" + rid[0] + ", tupleId=" + rid[1]);

          if (isClustered) {
            if (currentTupleIndex < allTuples.size()) {
              Tuple tuple = new Tuple(allTuples.get(currentTupleIndex++));
              int key = tuple.getElementAtIndex(getKeyColumnIndex());
              if (key > highKey) return null;
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
    // Clear any existing buffer state
    buffer.clear();

    // Calculate exact offset for the tuple
    long pageOffset = (long) rid[0] * PAGE_SIZE;
    dataFileRAF.seek(pageOffset);

    // Read page header
    int numAttrs = dataFileRAF.readInt();
    int numTuples = dataFileRAF.readInt();

    // Calculate precise tuple offset
    int tupleSize = numAttrs * 4; // Each attribute is 4 bytes
    long tupleOffset = pageOffset + 8 + ((long) rid[1] * tupleSize);

    // Seek directly to tuple position
    dataFileRAF.seek(tupleOffset);

    // Read tuple data
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
      currentRids.add(new int[] {pageId, tupleId});
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
    // Since the index is built on a specific column, we need to find its position
    // in the schema
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
