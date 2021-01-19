package com.johnlpage.pocdriver;


import java.util.*;

import org.bson.BsonBinarySubType;
import org.bson.Document;
import org.bson.types.Binary;

import de.svenjacobs.loremipsum.LoremIpsum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//A Test Record is a MongoDB Record Object that is self populating

public class TestRecord {
	Logger logger;
	Document internalDoc;
	private Random rng;
	private static ArrayList<ArrayList<Integer>> ar;
	private static String loremText = null;

	private static Binary blobData = null;

	private String CreateString(int length) {

		if( loremText == null )
		{
			loremText = "";
			LoremIpsum loremIpsum = new LoremIpsum();

			loremText = loremIpsum.getWords( 1000 );
		}

		StringBuilder sb = new StringBuilder();
		Double d = rng.nextDouble();

		int loremLen = 512;
		int r = (int) Math.abs(Math.floor( d * (loremText.length() - (loremLen + 20))));
		int e = r + loremLen;

		while(loremText.charAt(r) != ' ') r++; r++;
		while(loremText.charAt(e) != ' ') e++;
		String chunk = loremText.substring(r, e);

		sb.append(chunk);


		//Double to size

		while(sb.length() < length)
		{
			logger.info(" SB " + sb.length() + " of " + length);
			sb.append(sb.toString());
		}

		//Trim to fit
		String rs = sb.toString().substring(0,length);

		//Remove partial words
		r=0;
		e=rs.length() -1;
		while(rs.charAt(e) != ' ') e--;
		rs = rs.substring(r, e);
		return rs;
	}

	// This needs to be clever as we really need to be able to
	// Say - assuming nothing was removed - what is already in the DB
	// Therefore we will have a one-up per thread
	// A thread starting will find out what it's highest was

	private void AddOID(int workerid, int sequence) {
		Document oid = new Document("w",workerid).append("i", sequence);
		internalDoc.append("_id", oid);
	}

	// Just so we always know what the type of a given field is
	// Useful for querying, indexing etc

	private static int getFieldType(int fieldno) {
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

	TestRecord(POCTestOptions testOpts) {
		
		this(testOpts.numFields, testOpts.depth, testOpts.textFieldLen, testOpts.workingset, 0,
				testOpts.NUMBER_SIZE, new int[]{testOpts.arraytop, testOpts.arraynext}, testOpts.blobSize);
	}

	TestRecord(int nFields, int depth, int stringLength, int workerID, int sequence, long numberSize, int[] array, int binsize) {
		logger = LoggerFactory.getLogger(TestRecord.class);
		internalDoc = new Document();
		rng = new Random();

		// Always a field 0
		AddOID(workerID, sequence);

		addFields(internalDoc, 0, nFields, depth, stringLength, numberSize);

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
		if (blobData == null) {
			byte[] data = new byte[binsize * 1024];
			rng.nextBytes(data);
			blobData = new Binary(BsonBinarySubType.BINARY, data);
		}

		internalDoc.append("bin", blobData);
	}

	/**
	 * @param seq	 The sequence for this document as a whole
	 * @param nFields The numbers of fields for this sub-document
	 * @return the number of new fields added
	 */
	private int addFields(Document doc, int seq, int nFields, int depth, int stringLength, long numberSize) {
		int fieldNo = seq;
		if (depth > 0) {
			// we need to create nodes not leaves
			int perLevel = (int) Math.pow(nFields, 1f / (depth + 1));
			for (int i = 0; i < perLevel; i++) {
				Document node = new Document();
				doc.append("node" + i, node);
				fieldNo += addFields(node, fieldNo, nFields / perLevel, depth - 1, stringLength, numberSize);
			}
		}
		// fields
		while (fieldNo < nFields + seq) {
			int fType = getFieldType(fieldNo);
			if (fType == 0) {
				// Field should always be a long this way

				long r = (long) Math.abs(Math.floor(rng.nextGaussian()
						* numberSize));

				doc.append("fld" + fieldNo, r);
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
				doc.append("fld" + fieldNo, now);
			} else {
				// put in a string
				String fieldContent = CreateString(stringLength);
				doc.append("fld" + fieldNo, fieldContent);
			}
			fieldNo++;
		}
		return fieldNo - seq;
	}

    public List<String> listFields() {
        List<String> fields = new ArrayList<String>();
        collectFields(internalDoc, "", fields);
        return fields;
    }

    private void collectFields(Document doc, String prefix, List<String> fields) {
        Set<String> keys = doc.keySet();
        for (String key : keys) {
            if (key.startsWith("fld")) {
                fields.add(prefix + key);
            } else if (key.startsWith("node")) {
                // node
                Document node = (Document) doc.get(key);
                collectFields(node, prefix + key + ".", fields);
            }
        }
    }

}
