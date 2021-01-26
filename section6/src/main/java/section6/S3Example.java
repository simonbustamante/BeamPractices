package section6;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

//the AWS credentials are set on Run As -> Run Configurations
//--AWSAccessKey=AKIAYVCXR6BU5UEFTQG5 --AWSSecretKey=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 --awsRegion=us-east-2

public class S3Example {

	public static void main(String[] args) {
		
		Options myOption=PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline p = Pipeline.create(myOption);
		
		AWSCredentials awsCredObject = new BasicAWSCredentials(myOption.getAWSAccessKey(), myOption.getAWSSecretKey());
		
		myOption.setAwsCredentialsProvider(new AWSStaticCredentialsProvider(awsCredObject));
		
		PCollection<String> pInput=p.apply(TextIO.read().from("s3://govsol-bucket/beam/user_order.csv"));
		
		pInput.apply(ParDo.of(new DoFn<String, Void>() {
			
			@ProcessElement
			public void processElement(ProcessContext c) {
				System.out.println(c.element());
			}
		}));
		
		p.run();
	}
}