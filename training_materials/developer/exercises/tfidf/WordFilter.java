public class WordFilter {

	/* Given a string, find if it is of interest,
 	 * and/or convert to one of interest.  
 	 *
 	 * E.g.:
         * 	Eliminate words in all uppercase.
         * 	Eliminate punctuation from words.
         * 	Could eliminate stop words.
         */ 

	public static boolean isGood(String word) {
		
		boolean result = true;

		// Reject words in all uppercase
		// (except for the pronoun "I").
		if (word.equals(word.toUpperCase())) {
			if (! word.equals("I")) {
				result = false;
			}
		}

		// Reject non-alphabetic tokens.
		word = word.replaceAll("[^A-Za-z]", "");
		if (word.length() == 0) {
			result = false;
		}

		return result;
	}

	public static String word2GoodWord(String word) {

		// Convert word to lowercase, and 
		// remove non-alphabetic characters.
		word = word.toLowerCase();
		word = word.replaceAll("[^a-z]", "");

		return word;
	}
}
