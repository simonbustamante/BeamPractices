package section6;

import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.PipelineOptions;

public interface Options extends PipelineOptions,S3Options {

	void setAWSAccessKey(String val);
	String getAWSAccessKey();
	
	void setAWSSecretKey(String val);
	String getAWSSecretKey();
	
	void setAwsRegion(String value);
	String getAwsRegion();

}
