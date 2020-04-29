package labTwo;

public class Bigram implements Comparable<Bigram> {
	private String bigram; // space seperated
	private String leftWord;
	private String rightWord;
	private int occurrence;

	Bigram(String objectString) {
		String[] line = objectString.split(",");
		leftWord = line[0];
		rightWord = line[1];
		bigram = leftWord + " " + rightWord;
		occurrence = Integer.parseInt(line[2]);
	}

	Bigram(String wordPair, int count) {
		bigram = wordPair;
		occurrence = count;
	}

	public String getBigram() {
		return bigram;
	}

	public int getOccurrence() {
		return occurrence;
	}

	@Override
	public String toString() {
		return leftWord + "," + rightWord + "," + occurrence;
	}

	@Override
	public int compareTo(Bigram compared) {
		if (this.getOccurrence() < compared.getOccurrence())
			return -1;
		else if (this.getOccurrence() > compared.getOccurrence())
			return 1;
		else
			return 0;
	}
}