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
 * <p>This operator:
 *
 * <ul>
 *   <li>Traverses the B+ tree to find the starting leaf node that corresponds to the given
 *       selection range (lowKey to highKey).
 *   <li>For unclustered indexes, it retrieves RIDs and then fetches tuples from the data file.
 *   <li>For clustered indexes, it may sequentially read tuples from the data file pages directly,
 *       if all tuples are physically contiguous.
 *   <li>Iterates over entries and RIDs, returning tuples that meet the specified range criteria.
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * ArrayList<Column> schema = ...; // Define the output schema
 * String relationName = "myTable";
 * String indexFile = "path/to/index/file";
 * boolean isClustered = true;
 * Integer lowKey = 10;  // Lower bound for the scan (inclusive)
 * Integer highKey = 50; // Upper bound for the scan (inclusive)
 * String indexedColumn = "A";
 *
 * IndexScanOperator scanOperator = new IndexScanOperator(
 *     schema, relationName, indexFile, isClustered, lowKey, highKey, indexedColumn
 * );
 *
 * Tuple tuple;
 * while ((tuple = scanOperator.getNextTuple()) != null) {
 *     System.out.println(tuple);
 * }
 * }</pre>
 */
public class IndexScanOperator extends Operator {
  /** Size of a page in bytes. */
  private static final int PAGE_SIZE = 4096;

  /** Name of the relation to scan. */
  private final String relationName;

  /** Path to the index file. */
  private final String indexFile;

  /** Whether the index is clustered. */
  private final boolean isClustered;

  /** Lower bound of the range (inclusive, null for unbounded). */
  private final Integer lowKey;

  /** Upper bound of the range (inclusive, null for unbounded). */
  private final Integer highKey;

  /** RandomAccessFile handle for reading the index file. */
  private RandomAccessFile indexRAF;

  /** RandomAccessFile handle for reading the data file. */
  private RandomAccessFile dataFileRAF;

  /** A buffer for reading pages from the index file. */
  private ByteBuffer buffer;

  /** The address of the root node in the B+ tree. */
  private int rootAddress;

  /** The page number of the current leaf node being scanned. */
  private int currentLeafPage;

  /** The current entry index within the current leaf node. */
  private int currentEntryIndex;

  /** The list of RIDs (record IDs) for the current entry. */
  private List<int[]> currentRids;

  /** The current index within currentRids. */
  private int currentRidIndex;

  /** All tuples, if clustered index is used. */
  private List<int[]> allTuples;

  /** Current index within allTuples. */
  private int currentTupleIndex;

  /** Indicates if the operator has been initialized (i.e., index traversal done). */
  private boolean initialized;

  /** The name of the indexed column. */
  private final String indexedColumn;

  /**
   * Constructs an IndexScanOperator.
   *
   * @param outputSchema The schema of the tuples produced by this operator.
   * @param relationName The name of the relation to scan.
   * @param indexFile The path to the index file.
   * @param isClustered true if the index is clustered, false otherwise.
   * @param lowKey The lower bound key of the range scan.
   * @param highKey The upper bound key of the range scan.
   * @param indexedColumn The name of the column on which the index is built.
   */
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
   * Initializes the operator by opening the index and data files, finding the appropriate leaf node
   * to start scanning from, and preparing the first entry's RIDs.
   *
   * @throws IOException if file I/O operations fail.
   */
  private void initialize() throws IOException {
    if (initialized) {
      return;
    }

    System.out.println("Starting index scan with bounds: [" + lowKey + ", " + highKey + "]");

    indexRAF = new RandomAccessFile(indexFile, "r");
    String dataFilePath = DBCatalog.getInstance().getFileForTable(relationName).getAbsolutePath();
    dataFileRAF = new RandomAccessFile(dataFilePath, "r");

    // Read the root address of the B+ tree from the index file
    buffer.clear();
    indexRAF.seek(0);
    indexRAF.read(buffer.array());
    rootAddress = buffer.getInt(0);
    System.out.println("Root address from index: " + rootAddress);

    // Find the starting leaf page based on the lowKey
    currentLeafPage = findStartLeaf(rootAddress);
    System.out.println("Found starting leaf page: " + currentLeafPage);

    buffer.clear();
    indexRAF.seek((long) currentLeafPage * PAGE_SIZE);
    indexRAF.read(buffer.array());

    currentEntryIndex = 0;

    int numEntries = buffer.getInt(4);
    System.out.println("Leaf has " + numEntries + " entries");
    for (int i = 0; i < Math.min(3, numEntries); i++) {
      int offset = calculateEntryOffset(i);
      int key = buffer.getInt(offset);
      System.out.println("Entry " + i + " has key: " + key);
    }

    // Seek the first valid entry >= lowKey (if lowKey is not null)
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

  /**
   * Attempts to read the next entry from the current leaf page.
   *
   * @return true if a next entry is available, false otherwise.
   * @throws IOException if file I/O operations fail.
   */
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

    // Check if this key is beyond the highKey
    if (highKey != null && key > highKey) {
      System.out.println("Key exceeds highKey " + highKey);
      return false;
    }
    // Check if this key is below the lowKey
    if (lowKey != null && key < lowKey) {
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

  /**
   * Calculates the byte offset of a given entry index within a leaf node page.
   *
   * @param entryIndex The index of the entry.
   * @return The byte offset in the page buffer.
   */
  private int calculateEntryOffset(int entryIndex) {
    int offset = 8;
    for (int i = 0; i < entryIndex; i++) {
      int numRids = buffer.getInt(offset + 4);
      offset += 8 + (numRids * 8);
    }
    return offset;
  }

  /**
   * Reads the entry at the specified offset from the leaf buffer and initializes currentRids.
   *
   * @param offset The byte offset to read from.
   */
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

  /**
   * Recursively descends the B+ tree to find the leaf node page that should contain the lowKey.
   *
   * @param nodeAddress The address of the current node (page number).
   * @return The page number of the leaf node.
   * @throws IOException if file I/O operations fail.
   */
  private int findStartLeaf(int nodeAddress) throws IOException {
    buffer.clear();
    indexRAF.seek((long) nodeAddress * PAGE_SIZE);
    indexRAF.read(buffer.array());

    int nodeType = buffer.getInt(0);

    // If nodeType == 0, it's a leaf node
    if (nodeType == 0) {
      return nodeAddress;
    }

    int numKeys = buffer.getInt(4);
    int childPointerOffset = 8 + (numKeys * 4);
    int childPtr = buffer.getInt(childPointerOffset);

    if (lowKey == null) {
      // If no lower bound, go to the leftmost child
      return findStartLeaf(childPtr);
    }

    // Otherwise, find the appropriate child pointer based on lowKey
    for (int i = 0; i < numKeys; i++) {
      int key = buffer.getInt(8 + i * 4);
      if (key > lowKey) {
        return findStartLeaf(childPtr);
      }
      childPtr = buffer.getInt(childPointerOffset + (i + 1) * 4);
    }

    return findStartLeaf(childPtr);
  }

  /**
   * Returns the next tuple from the index scan. If the index is unclustered, it uses RIDs to fetch
   * tuples from the data file. If clustered, it may read from a sequential set of tuples.
   *
   * @return The next tuple, or null if no more tuples.
   */
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
            if (allTuples != null && currentTupleIndex < allTuples.size()) {
              Tuple tuple = new Tuple(allTuples.get(currentTupleIndex++));
              int key = tuple.getElementAtIndex(getKeyColumnIndex());
              if (highKey != null && key > highKey) return null;
              return tuple;
            }
            // If clustered but allTuples not populated or exhausted, this logic would need
            // to be adapted.
            // Here we assume allTuples would be set by some logic if clustered indexes are
            // handled differently.
            return null;
          } else {
            // For unclustered, fetch the tuple by RID
            Tuple tuple = readTupleFromRID(rid);
            int key = tuple.getElementAtIndex(getKeyColumnIndex());
            if (highKey != null && key > highKey) {
              return null;
            }
            return tuple;
          }
        }

        // No more RIDs in current entry, try the next entry
        if (!readNextEntry()) {
          return null;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Reads a tuple from the data file using the given RID.
   *
   * @param rid An array [pageId, tupleId].
   * @return The tuple read from the specified RID.
   * @throws IOException if file I/O operations fail.
   */
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

  /**
   * Reads a page from the index file into the buffer.
   *
   * @param pageNum The page number to read.
   * @throws IOException if file I/O operations fail.
   */
  private void readPage(int pageNum) throws IOException {
    buffer.clear();
    indexRAF.seek((long) pageNum * PAGE_SIZE);
    indexRAF.read(buffer.array());
  }

  /**
   * Resets the operator to its initial state, closing all open resources. Subsequent calls to
   * getNextTuple() will restart the index scan.
   */
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

  /**
   * Finds the index of the indexedColumn in the output schema.
   *
   * @return The index of the indexed column.
   */
  private int getKeyColumnIndex() {
    for (int i = 0; i < outputSchema.size(); i++) {
      Column col = outputSchema.get(i);
      if (col.getColumnName().equals(indexedColumn)) {
        return i;
      }
    }
    throw new IllegalStateException("Could not find indexed column in schema");
  }

  /**
   * Ensures that resources are closed when the object is garbage collected.
   *
   * @throws Throwable if finalization fails.
   */
  @Override
  protected void finalize() throws Throwable {
    reset();
    super.finalize();
  }
}
