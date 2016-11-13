
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CSE535Assignment {

	static class Postings {

		int docId;
		int termFreq;

		Postings(int docId, int termFreq) {
			this.docId = docId;
			this.termFreq = termFreq;
		}
	}

	static class PostingsTopK {

		String term;
		int docFreq;

		PostingsTopK(String term, int docFreq) {
			this.term = term;
			this.docFreq = docFreq;
		}

	}

	static class PostingsListComparator implements Comparator<PostingsTopK> {

		@Override
		public int compare(PostingsTopK k1, PostingsTopK k2) {
			if (k1.docFreq < k2.docFreq) {
				return 1;
			} else {
				return -1;
			}
		}

	}

	static class TermFrequencyComparator implements Comparator<Postings> {

		@Override
		public int compare(Postings p1, Postings p2) {
			if (p1.termFreq < p2.termFreq) {
				return 1;
			} else {
				return -1;
			}
		}

	}

	static class IncreasingDocIDComparator implements Comparator<Postings> {

		@Override
		public int compare(Postings p1, Postings p2) {
			if (p1.docId > p2.docId) {
				return 1;
			} else {
				return -1;
			}
		}

	}

	private static BufferedReader reader1;
	private static BufferedReader reader2;
	private final static String POSTINGS_REGEX_1 = "\\[(.*?)\\]";
	private static int tAATOrComparisons = 0;
	private static int tAATAndComparisons = 0;

	LinkedList<Postings> postings = new LinkedList<Postings>();

	public static void main(String[] args) throws IOException {

		int size = Integer.parseInt(args[2]);
		String fileName = args[1];

		FileInputStream fis1 = new FileInputStream(args[0]);
		reader1 = new BufferedReader(new InputStreamReader(fis1));

		FileInputStream fis2 = new FileInputStream(args[3]);
		reader2 = new BufferedReader(new InputStreamReader(fis2));

		PrintWriter writer = new PrintWriter(fileName, "UTF-8");

		handleInputData(reader1, reader2, writer, size);
		writer.close();
	}

	// method handles input data coming from files
	private static void handleInputData(BufferedReader reader1, BufferedReader reader2, PrintWriter writer,
			int topKSize) throws IOException {

		ArrayList<LinkedList<Postings>> list1 = new ArrayList<LinkedList<Postings>>();
		ArrayList<PostingsTopK> topKTermsList = new ArrayList<PostingsTopK>();
		HashMap<String, LinkedList<Postings>> postingsMap = new HashMap<String, LinkedList<Postings>>();
		LinkedList<Postings> postingsList = new LinkedList<Postings>();
		String line = null;

		while ((line = reader1.readLine()) != null) {
			String[] elements = line.split("\\\\");
			String key = elements[0];
			String termName = elements[0];
			String docFrequency = handleDocumentFrequency(elements[1]);
			String trimmedPostings = elements[2].trim();
			postingsList = buildLinkedList(trimmedPostings);
			postingsMap.put(key, postingsList);
			list1.add(postingsList);
			topKTermsList.add(new PostingsTopK(termName, (Integer.parseInt(docFrequency))));
			Collections.sort(topKTermsList, new PostingsListComparator());
		}

		printTopKTermsList(topKTermsList, topKSize, writer);

		while ((line = reader2.readLine()) != null) {
			String[] inputQueryTerms = line.split(" ");
			ArrayList<String> inputQueryTermsList = new ArrayList<String>(inputQueryTerms.length);
			Collections.addAll(inputQueryTermsList, inputQueryTerms);

			for (int i = 0; i < inputQueryTermsList.size(); i++) {
				writer.println(" ");
				writer.println("FUNCTION: getPostings " + inputQueryTerms[i]);
				printPostingsByDocId(inputQueryTerms[i], postingsMap, writer);
				printPostingsByTermFrequency(inputQueryTerms[i], postingsMap, writer);
			}

			writer.println();
			writer.print("FUNCTION: termAtATimeQueryAnd ");
			for (int i = 0; i < inputQueryTermsList.size(); i++) {
				writer.print(inputQueryTermsList.get(i));
				if (i != (inputQueryTermsList.size() - 1)) {
					writer.print(" , ");
				}
			}
			writer.println();
			handleTermAtATimeAND(inputQueryTermsList, postingsMap, writer);
			writer.println();

			writer.print("FUNCTION: termAtATimeQueryOr ");
			for (int i = 0; i < inputQueryTermsList.size(); i++) {
				writer.print(inputQueryTermsList.get(i));
				if (i != (inputQueryTermsList.size() - 1)) {
					writer.print(" , ");
				}
			}
			writer.println();
			handleTermAtATimeOR(inputQueryTermsList, postingsMap, writer);
		}
	}

	// method handles TermATATimeAND taking inputs as input query terms and
	// postingsMap
	private static void handleTermAtATimeAND(ArrayList<String> inputQueryTermsList,
			HashMap<String, LinkedList<Postings>> postingsMap, PrintWriter writer) {
		long totalTime = 0;
		ArrayList<Integer> docIds = new ArrayList<Integer>();
		LinkedList<Postings> tempLinkedList = new LinkedList<Postings>();
		for (int i = 0; i < inputQueryTermsList.size(); i++) {
			if (i == 0) {
				LinkedList<Postings> postingsList1 = postingsMap.get(inputQueryTermsList.get(i));
				if (postingsList1 != null) {
					Collections.sort(postingsList1, new TermFrequencyComparator());
				}
				tempLinkedList = postingsList1;
				if (postingsList1 != null) {
					Collections.sort(postingsList1, new IncreasingDocIDComparator());
				}
			}
			if (!(i + 1 < inputQueryTermsList.size())) {
				break;
			} else {
				LinkedList<Postings> postingsList2 = postingsMap.get(inputQueryTermsList.get(i + 1));
				if (postingsList2 != null) {
					Collections.sort(postingsList2, new TermFrequencyComparator());
				}

				long startTime = System.currentTimeMillis();
				tempLinkedList = evaluateTermAtATimeAND(tempLinkedList, postingsList2, docIds);
				long endTime = System.currentTimeMillis();
				totalTime = totalTime + (endTime - startTime);
			}
		}
		if (tempLinkedList == null || tempLinkedList.isEmpty()) {
			writer.print("terms not found");
		} else {
			Collections.sort(tempLinkedList, new IncreasingDocIDComparator());
			long finalDuration = (totalTime / 1000);
			writer.println(tempLinkedList.size() + " documents are found");
			writer.println(tAATAndComparisons + " comparisons are made");
			writer.println(finalDuration + " seconds are used");
			writer.print("Result: ");
			for (int i = 0; i < tempLinkedList.size(); i++) {
				writer.print(tempLinkedList.get(i).docId);
				if (i != (tempLinkedList.size() - 1)) {
					writer.print(" , ");
				}
			}
		}
	}

	// method handles TermATATimeOR taking inputs as input query terms and
	// postingsMap
	private static void handleTermAtATimeOR(ArrayList<String> inputQueryTermsList,
			HashMap<String, LinkedList<Postings>> postingsMap, PrintWriter writer) {
		long totalTime = 0;
		ArrayList<Integer> docIds = new ArrayList<Integer>();
		LinkedList<Postings> tempLinkedList = new LinkedList<Postings>();
		for (int i = 0; i < inputQueryTermsList.size(); i++) {
			if (i == 0) {
				LinkedList<Postings> postingsList1 = postingsMap.get(inputQueryTermsList.get(i));
				if (postingsList1 != null) {
					Collections.sort(postingsList1, new TermFrequencyComparator());
				}

				tempLinkedList = postingsList1;
				if (postingsList1 != null) {
					Collections.sort(postingsList1, new IncreasingDocIDComparator());
				}
			}
			if (!(i + 1 < inputQueryTermsList.size())) {
				break;
			} else {
				LinkedList<Postings> postingsList2 = postingsMap.get(inputQueryTermsList.get(i + 1));
				if (postingsList2 != null) {
					Collections.sort(postingsList2, new TermFrequencyComparator());
				}

				long startTime = System.currentTimeMillis();
				tempLinkedList = evaluateTermAtATimeOR(tempLinkedList, postingsList2, docIds);
				long endTime = System.currentTimeMillis();
				totalTime = totalTime + (endTime - startTime);
			}
		}
		if (tempLinkedList == null || tempLinkedList.isEmpty()) {
			writer.print("terms not found");
		} else {
			Collections.sort(tempLinkedList, new IncreasingDocIDComparator());
			long finalDuration = (totalTime / 1000);
			writer.println(tempLinkedList.size() + " documents are found");
			writer.println(tAATOrComparisons + " comparisons are made");
			writer.println(finalDuration + " seconds are used");
			writer.print("Result: ");
			for (int i = 0; i < tempLinkedList.size(); i++) {
				writer.print(tempLinkedList.get(i).docId);
				if (i != (tempLinkedList.size() - 1)) {
					writer.print(" , ");
				}
			}
		}

	}

	// method handles getPostings by increasing DocId
	private static void printPostingsByDocId(String queryTerm, HashMap<String, LinkedList<Postings>> postingsMap,
			PrintWriter writer) {
		boolean isPresent = false;
		for (String key : postingsMap.keySet()) {
			if (queryTerm.equals(key)) {
				writer.print("Ordered by doc IDs: ");
				int counter = 0;
				for (Postings p : postingsMap.get(key)) {
					counter++;
					writer.print(Integer.toString(p.docId));
					if (counter != (postingsMap.get(key).size())) {
						writer.print(" , ");
					}
					isPresent = true;
				}
			}
		}
		if (!isPresent) {
			writer.print("terms not found");
		}
	}

	// method handles getPostings by decreasing Term Frequency
	private static void printPostingsByTermFrequency(String queryTerm,
			HashMap<String, LinkedList<Postings>> postingsMap, PrintWriter writer) {
		for (String key : postingsMap.keySet()) {
			if (queryTerm.equals(key)) {
				writer.println(" ");
				writer.print("Ordered by TF: ");
				Collections.sort(postingsMap.get(key), new TermFrequencyComparator());
				int counter = 0;
				for (Postings p : postingsMap.get(key)) {
					counter++;
					writer.print(Integer.toString(p.docId));
					if (counter != (postingsMap.get(key).size())) {
						writer.print(" , ");
					}
				}
			}
		}
	}

	// builds a Linked List of Postings
	private static LinkedList<Postings> buildLinkedList(String extractPostings) {
		LinkedList<Postings> postingsObject = new LinkedList<Postings>();
		String postingsText;
		String[] tempPostingsArray;
		Pattern p = Pattern.compile(POSTINGS_REGEX_1);
		Matcher m = p.matcher(extractPostings);
		while (m.find()) {
			postingsText = m.group(1);
			tempPostingsArray = postingsText.split(",");
			for (int i = 0; i < tempPostingsArray.length; i++) {
				String tempPostings1 = tempPostingsArray[i];
				String newString = tempPostings1.trim();
				String[] postings1 = newString.split("/");
				postingsObject.add(new Postings(Integer.parseInt(postings1[0]), (Integer.parseInt(postings1[1]))));
			}
		}

		return postingsObject;
	}

	// method prints the Top K Terms
	private static void printTopKTermsList(ArrayList<PostingsTopK> topKTermsList, int topKSize, PrintWriter writer)
			throws IOException {
		// print top k terms
		writer.println("FUNCTION: getTopK " + Integer.toString(topKSize));
		writer.print("Result: ");
		for (int i = 0; i < topKSize; i++) {
			writer.print(topKTermsList.get(i).term);
			if (i != topKSize - 1) {
				writer.print(" , ");
			}
		}
	}

	// utility method which trims Document Frequency
	private static String handleDocumentFrequency(String tempDocFrequency) {
		String trimmedDocFrequency = tempDocFrequency.substring(1);
		return trimmedDocFrequency;
	}

	// method evaluates TermATATimeAND taking input as 2 Posting Lists. Method
	// also handles the null case.
	private static LinkedList<Postings> evaluateTermAtATimeAND(LinkedList<Postings> postingsList1,
			LinkedList<Postings> postingsList2, ArrayList<Integer> docIds) {
		LinkedList<Postings> resultList = new LinkedList<Postings>();
		int comparisons = 0;

		if ((postingsList1 == null || postingsList1.size() == 0)
				&& (postingsList2 != null && postingsList2.size() > 0)) {

			return resultList;
		} else if ((postingsList2 == null || postingsList2.size() == 0)
				&& (postingsList1 != null && postingsList1.size() > 0)) {

			return resultList;
		} else if ((postingsList2 == null || postingsList2.size() == 0)
				&& (postingsList1 == null || postingsList1.size() == 0)) {
			return resultList;
		} else {
			for (int i = 0; i < postingsList1.size(); i++) {
				for (int j = 0; j < postingsList2.size(); j++) {
					comparisons++;
					if (postingsList1.get(i).docId == postingsList2.get(j).docId) {
						resultList.add(new Postings(postingsList1.get(i).docId, postingsList1.get(i).termFreq));
						break;
					}
				}
			}
		}

		tAATAndComparisons = comparisons;
		return resultList;
	}

	// method evaluates TermATATimeOR taking input as 2 Posting Lists. Method
	// also handles the null case.
	private static LinkedList<Postings> evaluateTermAtATimeOR(LinkedList<Postings> postingsList1,
			LinkedList<Postings> postingsList2, ArrayList<Integer> docIds) {
		LinkedList<Postings> resultList = new LinkedList<Postings>();
		ArrayList<Integer> storedIds = new ArrayList<Integer>();
		int comparisons = 0;

		if ((postingsList1 == null || postingsList1.size() == 0)
				&& (postingsList2 != null && postingsList2.size() > 0)) {

			resultList = postingsList2;
			return resultList;
		} else if ((postingsList2 == null || postingsList2.size() == 0)
				&& (postingsList1 != null && postingsList1.size() > 0)) {

			resultList = postingsList1;
			return resultList;
		} else if ((postingsList2 == null || postingsList2.size() == 0)
				&& (postingsList1 == null || postingsList1.size() == 0)) {

			return resultList;
		} else {
			for (int i = 0; i < postingsList1.size(); i++) {
				for (int j = 0; j < postingsList2.size(); j++) {
					comparisons++;
					if (postingsList1.get(i).docId == postingsList2.get(j).docId) {
						if (!docIdAlreadyExists(storedIds, postingsList1.get(i).docId)) {
							resultList.add(new Postings(postingsList1.get(i).docId, postingsList1.get(i).termFreq));
							storedIds.add(postingsList1.get(i).docId);
						}
					} else {
						if (!docIdAlreadyExists(storedIds, postingsList1.get(i).docId)) {
							resultList.add(new Postings(postingsList1.get(i).docId, postingsList1.get(i).termFreq));
							storedIds.add(postingsList1.get(i).docId);
						}
						if (!docIdAlreadyExists(storedIds, postingsList2.get(j).docId)) {
							resultList.add(new Postings(postingsList2.get(j).docId, postingsList2.get(j).termFreq));
							storedIds.add(postingsList2.get(j).docId);
						}
					}
				}
			}
		}

		tAATOrComparisons = comparisons;
		return resultList;
	}

	// utility method which checks whether maintains history of stored document
	// ids in temporary linked list
	private static boolean docIdAlreadyExists(ArrayList<Integer> storedIds, int x) {
		for (int i = 0; i < storedIds.size(); i++) {
			if (x == storedIds.get(i)) {
				return true;
			}
		}
		return false;
	}

}