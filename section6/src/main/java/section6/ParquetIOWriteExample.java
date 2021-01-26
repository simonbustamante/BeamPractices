package section6;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;



class PrintElem extends SimpleFunction<GenericRecord, Void>{
	
	@Override
	public Void apply(GenericRecord input) {
		// TODO Auto-generated method stub
			
		System.out.println(input.get("SessionId"));
		System.out.println("SessionId : "+input.get("SessionId"));
		System.out.println("UserId" + input.get("UserId"));
		System.out.println("UserName" + input.get("UserName"));
		System.out.println("VideoId" + input.get("VideoId"));	
		System.out.println("Duration" + input.get("Duration"));	
		System.out.println("StartedTime" + input.get("StartedTime"));		
		System.out.println("Sex" + input.get("Sex"));
		
		return null;
	}
}


public class ParquetIOWriteExample {

	public static void main(String[] args) {		
        
  		Pipeline p = Pipeline.create();  		


  		Schema schema = BeamCustUtil.getSchema();
  		
  		PCollection<GenericRecord> poutput=p.apply(ParquetIO.read(schema).from("/home/sabb/Documents/Beam/Section6/output-00000-of-00001.parquet"));
  		
  		poutput.apply(MapElements.via(new PrintElem()));
  		
  		p.run();
	}
}

