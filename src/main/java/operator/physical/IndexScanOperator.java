package operator.physical;

import common.DBCatalog;
import common.Tuple;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;

/**
 * The {@code IndexScanOperator} class implements an operator for scanning a relation using a B+
 * tree index. It supports both clustered and unclustered indices and provides range-based filtering
 * on indexed attributes.
 *
 * <p>The operator works by traversing the B+ tree to identify the relevant leaf nodes, and then
 * retrieves tuples based on the range specified by the low and high keys.
 *
 * <h2>Features</h2>
 *
 * <ul>
 *   <li>Supports clustered and unclustered indices.
 *   <li>Performs range-based scans using a lower and upper bound on the indexed attribute.
 *   <li>Efficiently reads tuples using a {@code RandomAccessFile} for unclustered indices or a
 *       {@link TupleReader} for clustered indices.
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * ArrayList<Column> schema = ...; // Define the output schema
 * String relationName = "myTable";
 * String indexFile = "path/to/index/file";
 * boolean isClustered = true;
 * Integer lowKey = 10;  // Lower bound for the scan
 * Integer highKey = 50; // Upper bound for the scan
 *
 * IndexScanOperator scanOperator = new IndexScanOperator(schema, relationName, indexFile, isClustered, lowKey, highKey);
 * Tuple tuple;
 * while ((tuple = scanOperator.getNextTuple()) != null) {
 *     System.out.println(tuple);
 * }
 * }</pre>
 */
public class IndexScanOperator extends Operator {
  private static final int PAGE_SIZE = 4096; // Size of a page in bytes
  private final String relationName; // Name of the relation to scan
  private final String indexFile; // Path to the index file
  private final boolean isClustered; // Whether the index is clustered
  private final Integer lowKey; // Lower bound of the range (inclusive, null for unbounded)
  private final Integer highKey; // Upper bound of the range (inclusive, null for unbounded)

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
  private final String indexedColumn;

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

  private void initialize() throws IOException {
    if (initialized) {
      return;
    }

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
    if (lowKey != null && key < lowKey) { // Check the lower bound
      currentEntryIndex++;
      return readNextEntry();
    }

    int numRids = buffer.getInt(offset + 4);
    System.out.println("Entry has " + numRids + " RIDs");
    offset += 8;

    currentRids.clear();
    for (int i = 0; i < numRids; i++) {
      int pageId = buffer.getInt(offset + i * 8);
      int tupleId = buffer.getInt(offset + i * 8 + 4);
      currentRids.add(new int[] {pageId, tupleId});
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
      currentRids.add(new int[] {pageId, tupleId});
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

  private void readPage(int pageNum) throws IOException {
    buffer.clear();
    indexRAF.seek(pageNum * PAGE_SIZE);
    indexRAF.read(buffer.array());
  }

  /** Resets the operator to its initial state, closing all open resources. */
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

  /** Ensures that resources are closed when the object is garbage collected. */
  @Override
  protected void finalize() throws Throwable {
    reset();
    super.finalize();
  }
}
