import java.util.Date;
import java.util.Random;

import org.apache.commons.codec.binary.Base64;
import org.bson.types.ObjectId;

import com.mongodb.*;


//A Test Record is a MongoDB Record Object that is self populating
@SuppressWarnings("serial")
public class TestRecord extends BasicDBObject {

	Random rng;
	final double VOCAB_SIZE = 100000;
	final double NUMBER_SIZE = 1000000;
	
	private String CreateString( int length , int fieldNo)
	{
		//We create a string by picking a number
		//Hashing it into a string
		//We use a gaussian distribution for the numbers
		StringBuilder sb = new StringBuilder();
		
		while(sb.length() < length)
		{
			long r = (long) Math.abs(Math.floor(rng.nextGaussian() * VOCAB_SIZE));
			if ( r % 5 != 0){
				sb.append("word" + r+ " ");
			} else {
				sb.append(r + " ");
				}
		}
		return sb.toString();
	}
	
	//Field size is a mean field size not an absolute
	
	public void AddOID()
	{
		ObjectId oid = new ObjectId();
		this.append("_id", oid);
	}
	public TestRecord( int nFields , int fieldSize)
	{
		rng = new Random();
		int fieldNo;
		for(fieldNo=0;fieldNo<nFields;fieldNo++)
		{
			if(fieldNo % 3 == 0)
			{
				long r = (long) Math.abs(Math.floor(rng.nextGaussian() * NUMBER_SIZE));
				this.append("fld"+fieldNo, r);
			} else
				if (fieldNo % 5 == 0)
				{
					long r = (long) Math.abs(Math.floor(rng.nextGaussian() * Long.MAX_VALUE));
					Date now = new Date(r);
					this.append("fld"+fieldNo, now);
				}
				else {
					//put in a string
					String fieldContent = CreateString( fieldSize , fieldNo );
					this.append("fld"+fieldNo, fieldContent);
				}
		}
		AddOID();
	}
}
