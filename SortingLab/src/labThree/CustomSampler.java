package labThree;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class CustomSampler{
	
	Random rand;
	RandomAccessFile inFile;
	String partitionsFile;
	
	public CustomSampler(String inPath, String outPath) throws IOException {
		inFile = new RandomAccessFile(inPath, "rwd");
		partitionsFile = outPath;
		rand = new Random();		
	}
	
	public boolean sample(int sampleSize, int numPartitions) throws IOException {
		//Create dir and file
		File dir = new File("temp");
		dir.mkdir();
		File file = new File(partitionsFile);
		file.createNewFile();
		long length = inFile.length();
		
		//Finds all potential keys by getting sampleSize number of entries
		ArrayList<String> list = new ArrayList<String>();
		for(int i =0; i < sampleSize; i++) {
			inFile.seek((Math.abs(rand.nextLong())%length)-1);
			String line = inFile.readLine();
			line = inFile.readLine();
			String[] lineTokens = line.split("\\s+|\\t+");
			String potentialSplitKey = lineTokens[0];
			list.add(potentialSplitKey);
		}
		
		//Sorts and selects the 9 splits then writes to file
		Collections.sort(list);
		BufferedWriter writer = new BufferedWriter(new FileWriter(partitionsFile, true));
		
		for(int i = 0; i < numPartitions-1; i++) {
			int partitionIndex = (sampleSize/numPartitions)*(i+1) - 1;
			writer.append(list.get(partitionIndex));
			writer.newLine();
			writer.flush();
		}
		writer.close();
		return true;
	}

}
