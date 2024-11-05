package operator.physical;

import common.DBCatalog;
import common.Tuple;
import file_management.TupleReader;
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
  public IndexScanOperator(
      ArrayList<Column> outputSchema,
      String relationName,
      String indexFile,
      boolean isClustered,
      Integer lowKey,
      Integer highKey) {
    super(outputSchema);
    this.relationName = relationName;
    this.indexFile = indexFile;
    this.isClustered = isClustered;
    this.lowKey = lowKey;
    this.highKey = highKey;
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
    readPage(0);
    rootAddress = buffer.getInt(0);

    // Traverse to first leaf node that could contain keys in our range
    currentLeafPage = findStartLeaf(rootAddress);
    currentEntryIndex = 0;

    if (isClustered) {
      // For clustered index, read all tuples at once since they're sequential
      readPage(currentLeafPage);
      if (!readNextEntry()) {
        return;
      }
      if (currentRids.isEmpty()) {
        return;
      }

      // Read all tuples using TupleReader
      try (TupleReader reader = new TupleReader(dataFilePath)) {
        allTuples = reader.readTuples();
        currentTupleIndex = currentRids.get(0)[1]; // Start from the first matching tuple
      }
    }

    initialized = true;
  }

  /** Finds the leaf node where scanning should start based on the low key. */
  private int findStartLeaf(int nodeAddress) throws IOException {
    readPage(nodeAddress);
    int nodeType = buffer.getInt(0);

    if (nodeType == 0) { // Leaf node
      return nodeAddress;
    }

    // Index node - find appropriate child
    int numKeys = buffer.getInt(4);
    int keyOffset = 8;
    int childOffset = 8 + (numKeys * 4);

    // If no low key, go to leftmost leaf
    if (lowKey == null) {
      return findStartLeaf(buffer.getInt(childOffset));
    }

    // Find first key greater than low key
    for (int i = 0; i < numKeys; i++) {
      if (buffer.getInt(keyOffset + i * 4) > lowKey) {
        return findStartLeaf(buffer.getInt(childOffset + i * 4));
      }
    }

    // All keys less than low key, use rightmost child
    return findStartLeaf(buffer.getInt(childOffset + numKeys * 4));
  }

  /** Reads the next data entry from current leaf node into currentRids. */
  private boolean readNextEntry() throws IOException {
    if (buffer.getInt(0) != 0) { // Not a leaf node
      return false;
    }

    int numEntries = buffer.getInt(4);
    if (currentEntryIndex >= numEntries) {
      return false;
    }

    // Calculate offset to current entry
    int offset = 8;
    for (int i = 0; i < currentEntryIndex; i++) {
      int numRids = buffer.getInt(offset + 4);
      offset += 8 + (numRids * 8); // key + numRids + (rids * 2 ints each)
    }

    int key = buffer.getInt(offset);
    // Check if we've passed high key
    if (highKey != null && key > highKey) {
      return false;
    }

    int numRids = buffer.getInt(offset + 4);
    offset += 8;

    currentRids.clear();
    for (int i = 0; i < numRids; i++) {
      int pageId = buffer.getInt(offset + i * 8);
      int tupleId = buffer.getInt(offset + i * 8 + 4);
      currentRids.add(new int[] {pageId, tupleId});
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

  @Override
  public Tuple getNextTuple() {
    try {
      if (!initialized) {
        initialize();
      }

      while (true) {
        // If we have more RIDs in current entry
        if (currentRidIndex < currentRids.size()) {
          int[] rid = currentRids.get(currentRidIndex++);

          if (isClustered) {
            // For clustered index, read sequentially from buffered tuples
            if (currentTupleIndex < allTuples.size()) {
              return new Tuple(allTuples.get(currentTupleIndex++));
            }
            return null;
          } else {
            // For unclustered index, seek to specific tuple
            dataFileRAF.seek(rid[0] * PAGE_SIZE + rid[1] * 4);
            // Read tuple at that position
            byte[] tupleData = new byte[outputSchema.size() * 4];
            dataFileRAF.read(tupleData);
            ByteBuffer tupleBuffer = ByteBuffer.wrap(tupleData);

            ArrayList<Integer> values = new ArrayList<>();
            for (int i = 0; i < outputSchema.size(); i++) {
              values.add(tupleBuffer.getInt(i * 4));
            }
            return new Tuple(values);
          }
        }

        // Need to move to next entry
        if (!readNextEntry()) {
          // No more entries in current leaf
          return null;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    reset();
    super.finalize();
  }
}
