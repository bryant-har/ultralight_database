import java.io.IOException;
import java.io.RandomAccessFile;

public class BTreePrinters {
    private static final String TEST_FILE =
      "/Users/nicholasvarela/Documents/Cornell/2024-2025/CS_4321/ultralight_database/src/test/resources/out/index_out.txt";
      public static void main(String[] args) throws IOException {
        printIndexFileContents(TEST_FILE);
    }
public static void printIndexFileContents(String fileName) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {
        // Read header
        int rootAddress = raf.readInt();
        int numLeaves = raf.readInt();
        int order = raf.readInt();
        System.out.println("Header:");
        System.out.println("Root address: " + rootAddress);
        System.out.println("Number of leaves: " + numLeaves);
        System.out.println("Order: " + order);

        // Read nodes
        int pageNumber = 1;
        while (raf.getFilePointer() < raf.length()) {
            raf.seek(pageNumber * 4096L);
            int nodeType = raf.readInt();
            if (nodeType == 0) {
                System.out.println("\nLeaf Node at page " + pageNumber + ":");
                int numEntries = raf.readInt();
                System.out.println("Number of entries: " + numEntries);
                for (int i = 0; i < numEntries; i++) {
                    int key = raf.readInt();
                    int numRids = raf.readInt();
                    System.out.print("Key: " + key + ", RIDs: ");
                    for (int j = 0; j < numRids; j++) {
                        int pageId = raf.readInt();
                        int tupleId = raf.readInt();
                        System.out.print("(" + pageId + "," + tupleId + ") ");
                    }
                    System.out.println();
                }
            } else if (nodeType == 1) {
                
                System.out.println("\nIndex Node at page " + pageNumber + ":");
                int numKeys = raf.readInt();
                System.out.print("Keys: ");
                for (int i = 0; i < numKeys; i++) {
                    System.out.print(raf.readInt() + " ");
                }
                System.out.print("\nChild Addresses: ");
                for (int i = 0; i <= numKeys; i++) {
                    System.out.print(raf.readInt() + " ");
                }
                System.out.println();
            }
            pageNumber++;
        }
    }
}

    
}
