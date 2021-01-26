package section6;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;


/** 
 * PArquet stores nested data structures in a flat columnar format
 * advantages of using parquet:
 * - Organizing by column allows for better compression, as data s more homogeneous. The Space saving
 * are very noticeable at the scale of a hadoop cluster
 * - I/O will be reduced as we can efficiently scan only a subset of the columns while reading data
 * - Better compression also reduces the bandwidth required to read the input
 * */

class BeamCustUtil{
	
	public static Schema  getSchema() {
		String SCHEMA_STRING =
			      "{\"namespace\": \"training.section6\",\n"
			          + " \"type\": \"record\",\n"
			          + " \"name\": \"ParquetExample\",\n"
			          + " \"fields\": [\n"
			          + "     {\"name\": \"SessionId\", \"type\": \"string\"},\n"
			          + "     {\"name\": \"UserId\", \"type\": \"string\"},\n"
			          + "     {\"name\": \"UserName\", \"type\": \"string\"},\n"
			          + "     {\"name\": \"VideoId\", \"type\": \"string\"},\n"
			          + "     {\"name\": \"Duration\", \"type\": \"int\"},\n"		          
			          + "     {\"name\": \"StartedTime\", \"type\": \"string\"},\n"		
			          + "     {\"name\": \"Sex\", \"type\": \"string\"}\n"				          
			          + " ]\n"
			          + "}";
		Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);
		return SCHEMA;
	}
}

class ConvertCsvToGeneric extends SimpleFunction<String, GenericRecord>{
	
	@Override
	public GenericRecord apply(String input) {
		// TODO Auto-generated method stub
		
		String arr[] = input.split(",");		
		Schema schema=BeamCustUtil.getSchema();
		GenericRecord record= new GenericData.Record(schema);
		record.put("SessionId",arr[0]);
		record.put("UserId", arr[1]);		
		record.put("UserName", arr[2]);		
		record.put("VideoId", arr[3]);
		record.put("Duration", Integer.parseInt(arr[4]));		
		record.put("StartedTime", arr[5]);	
		record.put("Sex", arr[6]);	
		
		return record;
	}
	
}

public class ParquetIOExample {

	public static void main(String[] args) {
		     
  		Pipeline p = Pipeline.create();
  		
  		Schema schema = BeamCustUtil.getSchema();
  		
  		PCollection<GenericRecord> pOutput=p.apply(TextIO.read().from("/home/sabb/Documents/Beam/Section6/user.csv"))
  		.apply(MapElements.via(new ConvertCsvToGeneric()))
  		.setCoder(AvroCoder.of(GenericRecord.class, schema));
  		
  		pOutput.apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(schema)).to("/home/sabb/Documents/Beam/Section6/parquetexample.parquet")
  				.withNumShards(1).withSuffix(".parquet"));
  		
  		
  		p.run();
	}
}