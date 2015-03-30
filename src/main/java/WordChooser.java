
public class WordChooser {
	
	public static class WordFreq {
		
		String word;
		int	   freq;
		private WordFreq( String w, int f) {
			this.word=w;
			this.freq=f;
		}
	}
	
	static WordFreq[] wordFrequencies =
		{
			new WordFreq("one",1)
		};
	
}
