package section4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

//Distinct<t> takes a PCollection<t> and returns a PColletion<t> that has all distinct elements of the input

public class DistinctExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Pipeline p = Pipeline.create();

		PCollection<String> pCustList = p.apply(TextIO.read().from("/home/sabb/Documents/Beam/Section4/Distinct.csv"));

		PCollection<String> uniqueCust=pCustList.apply(Distinct.<String>create());
				
		uniqueCust.apply(TextIO.write().to("/home/sabb/Documents/Beam/Section4/distinct_out.csv").withNumShards(1).withSuffix(".csv"));
		
		p.run();
	}

}