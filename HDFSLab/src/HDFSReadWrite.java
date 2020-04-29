import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class HDFSReadWrite {
	private static int numBytes = 1000;

	public static void writeAttempt() throws IOException{
		// The system configuration
		Configuration conf = new Configuration();
		// Get an instance of the Filesystem
		FileSystem fs = FileSystem.get(conf);

		String path_name = "/user/jijacobs/lab1/newfile";

		Path path = new Path(path_name);

		// The Output Data Stream to write into
		FSDataOutputStream file = fs.create(path);

		// Write some data
		file.writeChars("The first Hadoop Program!");

		// Close the file and the file system instance
		file.close();
		fs.close();
	}

	public static void readAttempt() throws IOException {
		// The system configuration
		Configuration conf = new Configuration();
		// Get an instance of the Filesystem
		FileSystem fs = FileSystem.get(conf);

		String path_name = "/cpre419/bigdata";
		Path path = new Path(path_name);

		byte[] buffer = new byte[numBytes];

		// The Output Data Stream to write into
		FSDataInputStream file = fs.open(path);
		file.read(1000000000, buffer, 0, numBytes);

		byte temp = buffer[0];
		for (int i = 0; i < numBytes - 1; i++) {
			temp = (byte) (temp ^ buffer[i + 1]);
		}

		System.out.println(String.format("%8s", Integer.toBinaryString(temp & 0xFF)).replace(' ', '0'));

		// Close the file and the file system instance
		file.close();
		fs.close();
	}
}