import static org.junit.jupiter.api.Assertions.*;

import common.BulkLoader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BulkLoaderTest {

  private static final String TEST_FILE =
      "/Users/nicholasvarela/Documents/Cornell/2024-2025/CS_4321/ultralight_database/src/test/resources/out/index_out.txt";
  private static final String TEST_FILE_INFO =
      "/Users/nicholasvarela/Documents/Cornell/2024-2025/CS_4321/ultralight_database/src/test/resources/samples/input/db_p3/index_info.txt";
  private static final int PAGE_SIZE = 4096;
  private BulkLoader bulkLoader;
  private RandomAccessFile raf;

  @BeforeEach
  void setUp() throws IOException {
    // Assuming BulkLoader constructor takes the output file name and order (d)
    try {
      bulkLoader = new BulkLoader(TEST_FILE_INFO, TEST_FILE);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create BulkLoader. Error: " + e.getMessage(), e);
    }

    try {
      raf = new RandomAccessFile(TEST_FILE, "rw");
    } catch (FileNotFoundException e) {
      throw new RuntimeException(
          "Failed to open file '"
              + TEST_FILE
              + "' for reading and writing. Error: "
              + e.getMessage(),
          e);
    }
  }

  @AfterEach
  void tearDown() throws IOException {
    if (raf != null) {
      raf.close();
    }
    // new File(TEST_FILE).delete();
  }

  @Test
  void testBulkLoadingAndSerialization() throws IOException {

    // Build and serialize the tree
    bulkLoader.buildAndSerialize();

    // Verify header page
    raf.seek(0);
    int rootAddress = raf.readInt();
    int numLeaves = raf.readInt();
    int treeOrder = raf.readInt();

    assertEquals(2, rootAddress, "Root address should be 2");
    assertEquals(2, numLeaves, "Number of leaves should be 2");
    assertEquals(1, treeOrder, "Tree order should be 1");

    // Verify leaf nodes
    verifyLeafNode(1, new int[] {1, 2}, new int[][] {{1, 1}, {1, 2}});
    verifyLeafNode(2, new int[] {3, 4}, new int[][] {{1, 3}, {1, 4}});

    // Verify root node (index node)
    raf.seek(rootAddress * PAGE_SIZE);
    assertEquals(1, raf.readInt(), "Root should be an index node");
    assertEquals(1, raf.readInt(), "Root should have 1 key");
    assertEquals(3, raf.readInt(), "Root's key should be 3");
    assertEquals(1, raf.readInt(), "Root's left child address should be 1");
    assertEquals(2, raf.readInt(), "Root's right child address should be 2");
  }

  private void verifyLeafNode(int address, int[] expectedKeys, int[][] expectedRids)
      throws IOException {
    raf.seek(address * PAGE_SIZE);
    assertEquals(0, raf.readInt(), "Should be a leaf node");
    int numEntries = raf.readInt();
    assertEquals(expectedKeys.length, numEntries, "Incorrect number of entries");

    for (int i = 0; i < numEntries; i++) {
      int key = raf.readInt();
      assertEquals(expectedKeys[i], key, "Incorrect key");
      assertEquals(1, raf.readInt(), "Should have 1 RID");
      assertEquals(expectedRids[i][0], raf.readInt(), "Incorrect page ID");
      assertEquals(expectedRids[i][1], raf.readInt(), "Incorrect tuple ID");
    }
  }
}
