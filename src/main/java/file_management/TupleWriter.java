package file_management;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.Pipe.SourceChannel;
import java.util.List;

public class TupleWriter {
  int PAGE_SIZE = 4096;
  private ByteBuffer buffer;
  private FileChannel fileChannel;
  private int numTuples;
  private int numTupleAttributes = 3;
  private List<int[]> tuples;
  private int currTuple;

  public TupleWriter(String filePath) throws IOException {
    File file = new File(filePath);
    file.getParentFile().mkdirs();
    if(!file.exists()) {
      file.createNewFile();
    } 
    FileOutputStream fileOutputStream = new FileOutputStream(file);
    this.fileChannel = fileOutputStream.getChannel();
    this.buffer = ByteBuffer.allocate(PAGE_SIZE);
    currTuple = 0;
  }

  public void writeTuples(int[] tuples) throws IOException {
    if (currTuple >= (PAGE_SIZE - 8) / numTupleAttributes * 4) {
      System.out.println("in flush math");
      flushPage();
    } else {
      for (int value : tuples) {
        buffer.putInt(value);
        System.out.println(value);
      }
      currTuple++;

    }

  }

  public void flushPage() throws IOException {
    buffer.putInt(0, currTuple);

    buffer.flip();
    fileChannel.write(buffer);
    actuallyClearBuffer();

  }

  /**
   * Clear the buffer, builds on native clear, but also sets all values to 0
   */
  public void actuallyClearBuffer() {
    buffer.clear();
    while (buffer.hasRemaining()) {
      buffer.put((byte) 0);
    }
    // Put pointer back at 0
    buffer.clear();

  }

  public void close() throws IOException {
    flushPage();
    fileChannel.close();
  }

}
