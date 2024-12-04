package operator.physical;

import common.DBCatalog;
import common.Tuple;
import file_management.TupleReader;
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

  private RandomAccessFile indexRAF; // RandomAccessFile for index file
  private RandomAccessFile dataFileRAF; // RandomAccessFile for data file
  private ByteBuffer buffer; // Buffer to store page data
  private int rootAddress; // Address of the root node in the B+ tree
  private int currentLeafPage; // Current leaf page being scanned
  private int currentEntryIndex; // Index of the current entry in the leaf page
  private List<int[]> currentRids; // List of record IDs (RIDs) for the current entry
  private int currentRidIndex; // Index of the current RID in the list
  private List<int[]> allTuples; // List of all tuples for clustered index
  private int currentTupleIndex; // Index of the current tuple for clustered index
  private boolean initialized; // Indicates if the operator has been initialized

  /**
   * Constructs an IndexScanOperator for scanning a relation using a B+ tree index.
   *
   * @param outputSchema The schema of the output tuples.
   * @param relationName The name of the relation to scan.
   * @param indexFile Path to the index file.
   * @param isClustered Whether the index is clustered.
   * @param lowKey Lower bound of the range to scan (null for unbounded).
   * @param highKey Upper bound of the range to scan (null for unbounded).
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
   *
   * @throws IOException If an I/O error occurs during initialization.
   */
  private void initialize() throws IOException {
    if (initialized) {
      return;
    }

    // Open the index and data files
    indexRAF = new RandomAccessFile(indexFile, "r");
    String dataFilePath = DBCatalog.getInstance().getFileForTable(relationName).getAbsolutePath();
    dataFileRAF = new RandomAccessFile(dataFilePath, "r");

    // Read the root address from the header page
    readPage(0);
    rootAddress = buffer.getInt(0);

    // Traverse to the starting leaf node
    currentLeafPage = findStartLeaf(rootAddress);
    currentEntryIndex = 0;

    if (isClustered) {
      // For clustered indices, read all tuples sequentially
      readPage(currentLeafPage);
      if (!readNextEntry()) {
        return;
      }
      if (currentRids.isEmpty()) {
        return;
      }

      // Use TupleReader to read tuples for clustered indices
      try (TupleReader reader = new TupleReader(dataFilePath)) {
        allTuples = reader.readTuples();
        currentTupleIndex = currentRids.get(0)[1]; // Start from the first matching tuple
      }
    }

    initialized = true;
  }

  /**
   * Finds the starting leaf node for the scan based on the low key.
   *
   * @param nodeAddress Address of the current node in the B+ tree.
   * @return Address of the starting leaf node.
   * @throws IOException If an I/O error occurs during traversal.
   */
  private int findStartLeaf(int nodeAddress) throws IOException {
    readPage(nodeAddress);
    int nodeType = buffer.getInt(0);

    if (nodeType == 0) { // Leaf node
      return nodeAddress;
    }

    // Index node - find the appropriate child node
    int numKeys = buffer.getInt(4);
    int keyOffset = 8;
    int childOffset = 8 + (numKeys * 4);

    // If no low key is specified, go to the leftmost child
    if (lowKey == null) {
      return findStartLeaf(buffer.getInt(childOffset));
    }

    // Find the child pointer before the first key greater than the low key
    int i;
    for (i = 0; i < numKeys; i++) {
      if (buffer.getInt(keyOffset + i * 4) > lowKey) {
        break;
      }
    }
    return findStartLeaf(buffer.getInt(childOffset + i * 4));
  }

  /**
   * Reads the next data entry from the current leaf node into {@code currentRids}.
   *
   * @return {@code true} if a valid entry was read; {@code false} otherwise.
   * @throws IOException If an I/O error occurs during reading.
   */
  private boolean readNextEntry() throws IOException {
    if (buffer.getInt(0) != 0) { // Not a leaf node
      return false;
    }

    int numEntries = buffer.getInt(4);
    if (currentEntryIndex >= numEntries) {
      // Move to the next leaf page
      int nextLeafPage = buffer.getInt(PAGE_SIZE - 4); // Last 4 bytes store the next leaf pointer
      if (nextLeafPage == -1) {
        return false;
      }
      readPage(nextLeafPage);
      currentLeafPage = nextLeafPage;
      currentEntryIndex = 0;
      return readNextEntry();
    }

    // Calculate the offset for the current entry
    int offset = 8;
    for (int i = 0; i < currentEntryIndex; i++) {
      int numRids = buffer.getInt(offset + 4);
      offset += 8 + (numRids * 8); // key + numRids + (rids * 2 ints each)
    }

    int key = buffer.getInt(offset);
    if (highKey != null && key > highKey) { // Check the upper bound
      return false;
    }
    if (lowKey != null && key < lowKey) { // Check the lower bound
      currentEntryIndex++;
      return readNextEntry();
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

  /**
   * Reads a page from the index file into the buffer.
   *
   * @param pageNum The page number to read.
   * @throws IOException If an I/O error occurs during reading.
   */
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

  /**
   * Retrieves the next tuple from the index scan.
   *
   * @return The next tuple, or {@code null} if no more tuples are available.
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
            if (currentTupleIndex < allTuples.size()) {
              return new Tuple(allTuples.get(currentTupleIndex++));
            }
            return null;
          } else {
            dataFileRAF.seek(rid[0] * PAGE_SIZE + rid[1] * 4);
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

        if (!readNextEntry()) {
          return null;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  /** Ensures that resources are closed when the object is garbage collected. */
  @Override
  protected void finalize() throws Throwable {
    reset();
    super.finalize();
  }
}
