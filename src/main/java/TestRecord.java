

import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import org.bson.Document;
import de.svenjacobs.loremipsum.LoremIpsum;



//A Test Record is a MongoDB Record Object that is self populating
@SuppressWarnings("serial")
public class TestRecord {

	Document internalDoc;
	Random rng;
	final double VOCAB_SIZE = 100000;
	final int loremLen = 512;
	static ArrayList<ArrayList<Integer>> ar;
	static String loremText = null;
	
	private String CreateString(int length, int fieldNo) {
		
		if( loremText == null )
		{
			loremText = new String();
			LoremIpsum loremIpsum = new LoremIpsum();
			//System.out.println("Generating sample data");
			loremText = loremIpsum.getWords( 1000 ); 
		}
		//System.out.println("Done");
		
		StringBuilder sb = new StringBuilder();
		Double d = rng.nextDouble();

		int r = (int) Math.abs(Math.floor( d * (loremText.length() - (loremLen + 20)))); 
		int e = r + loremLen;
		
		while(loremText.charAt(r) != ' ') r++; r++;
		while(loremText.charAt(e) != ' ') e++;;
		String chunk = loremText.substring(r, e);
	
		sb.append(chunk);
		
		
		//Double to size
		
		while(sb.length() < length)
		{
		//	System.out.println(" SB " + sb.length() + " of " + length);
			sb.append(sb.toString());
		}
		
		//Trim to fit
		String rs = sb.toString().substring(0,length);
		
		//Remove partial words
		r=0;
		e=rs.length() -1;
		while(rs.charAt(e) != ' ') e--;;
		rs = rs.substring(r, e);
		return rs;
	}

	public Document RemoveOID()
	{
		Document d = (Document) internalDoc.get("_id");
		internalDoc.remove("_id");
		return d;
	}

	// This needs to be clever as we really need to be able to
	// Say - assuming nothing was removed - what is already in the DB
	// Therefore we will have a one-up per thread
	// A thread starting will find out what it's highest was

	public void AddOID(int workerid, int sequence) {
		Document oid = new Document("w",workerid).append("i", sequence);
		internalDoc.append("_id", oid);
	}

	// Just so we always know what the type of a given field is
	// Useful for querying, indexing etc

	static public int getFieldType(int fieldno) {
		if (fieldno == 0) {
			return 0; // Int
		}

		if (fieldno == 1) {
			return 2; // Date
		}

		if (fieldno == 3) {
			return 1; // Text
		}

		if (fieldno % 3 == 0) {
			return 0; // Integer
		}

		if (fieldno % 5 == 0) {
			return 2; // Date
		}

		return 1; // Text
	}

	public TestRecord(int nFields, int fieldSize, int workerID, int sequence,
			long numberSize, int numShards, int[] array) {
		internalDoc = new Document();
		rng = new Random();
		int fieldNo;
		// Always a field 0
		AddOID(workerID, sequence);
		for (fieldNo = 0; (fieldNo < nFields || fieldNo == 0); fieldNo++) {
			int fType = getFieldType(fieldNo);
			if (fType == 0) {
				// Field should always be a long this way

				long r = (long) Math.abs(Math.floor(rng.nextGaussian()
						* numberSize));

				internalDoc.append("fld" + fieldNo, r);
			} else if (fieldNo == 1 || fType == 2) // Field 2 is always a date
													// as is every 5th
			{
				// long r = (long) Math.abs(Math.floor(rng.nextGaussian() *
				// Long.MAX_VALUE));
				Date now = new Date();
				// Subtract up to a few years
				long t = now.getTime();
				// Push it back 30 years or so
				t = (long) (t - Math
						.abs(Math.floor(rng.nextGaussian() * 100000000 * 3000)));
				now.setTime(t);
				internalDoc.append("fld" + fieldNo, now);
			} else {
				// put in a string
				String fieldContent = CreateString(fieldSize, fieldNo);
				internalDoc.append("fld" + fieldNo, fieldContent);
			}
		}
		if (array[0] > 0) {
			if (ar == null) {
				ar = new ArrayList<ArrayList<Integer>>(array[0]);
				for (int q = 0; q < array[0]; q++) {
					ArrayList<Integer> sa = new ArrayList<Integer>(array[1]);
					for (int w = 0; w < array[1]; w++) {
						sa.add(0);
					}
					ar.add(sa);
				}
			}
			internalDoc.append("arr", ar);
		}
	}

}
