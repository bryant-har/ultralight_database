package file_management;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

public class TupleWriter {
  int PAGE_SIZE = 4096;
  private ByteBuffer buffer;
  private FileChannel fileChannel;
  private int numTuples;
  private int numTupleAttributes = 3;
  private List<int[]> tuples;
  private int currTuple;
  private int currPage = 1;

  public TupleWriter(String filePath) throws IOException {
    File file = new File(filePath);
    file.getParentFile().mkdirs();
    if (!file.exists()) {
      file.createNewFile();
    }
    FileOutputStream fileOutputStream = new FileOutputStream(file);
    this.fileChannel = fileOutputStream.getChannel();
    this.buffer = ByteBuffer.allocate(PAGE_SIZE);
    actuallyClearBuffer();
    currTuple = 0;
  }

  public void writeTuple(int[] tuple) throws IOException {
    numTupleAttributes = tuple.length;
    if (8 + (currTuple + 1) * numTupleAttributes * 4 >= PAGE_SIZE) {
      flushPage();
    }
    int baseIndex = 8 + currTuple * numTupleAttributes * 4;
    for (int i = 0; i < tuple.length; i++) {
      if (baseIndex + i * 4 < buffer.capacity()) {
        buffer.putInt(baseIndex + i * 4, tuple[i]);
      } else {
        System.out.println("fejwiofew");
      }
    }
    currTuple++;
  }

  public void flushPage() throws IOException {
    buffer.putInt(0, numTupleAttributes);
    buffer.putInt(4, currTuple);
    currPage += 1;
    currTuple = 0;
    fileChannel.write(buffer);
    actuallyClearBuffer();
  }

  /** Clear the buffer, builds on native clear, but also sets all values to 0 */
  public void actuallyClearBuffer() throws IOException {
    buffer.clear();
    while (buffer.hasRemaining()) {
      buffer.put((byte) 0);
    }
    // buffer.flip();
    // fileChannel.write(buffer);

    // Put pointer back at 0
    buffer.clear();
  }

  public void close() throws IOException {
    flushPage();
    // Reset position to 0 for subsequent write operations
    fileChannel.close();
  }
}
