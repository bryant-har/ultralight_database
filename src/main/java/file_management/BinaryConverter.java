package file_management;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BinaryConverter {

  public static void main(String[] args) {
    String[][] tuples = {
      {"101", "2", "3"},
      {"102", "3", "4"},
      {"104", "104", "2"},
      {"103", "1", "1"},
      {"107", "2", "8"}
    };

    try (FileOutputStream fos = new FileOutputStream("output.bin")) {
      ByteBuffer buffer = ByteBuffer.allocate(4096); // One page is 4096 bytes

      for (String[] tuple : tuples) {
        for (String value : tuple) {
          int intValue = Integer.parseInt(value);
          buffer.putInt(intValue); // Write the integer as 4 bytes
        }

        // Check if the buffer is full (page size limit reached)
        if (buffer.position()
            >= 4096 - 12) { // Account for the 12 bytes of one tuple (3 integers x 4 bytes)
          buffer.flip(); // Switch to read mode for writing to the file
          fos.write(buffer.array()); // Write the full page to the file
          buffer.clear(); // Clear the buffer for the next page
        }
      }

      // Fill the rest of the last page with zeroes if needed
      if (buffer.position() > 0) {
        while (buffer.position() < 4096) {
          buffer.putInt(0); // Fill remaining bytes with zeroes
        }
        buffer.flip();
        fos.write(buffer.array()); // Write the last partially filled page
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
