/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.training.section10;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=beam-test-296614
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */

//this code allows to insert data from bucket to the BigQuery DWH
public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

  public static void main(String[] args) {
	  MyOption myOption=PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOption.class);
		myOption.setTempLocation("gs://bucket-beam-simon/input");
		myOption.setStagingLocation("gs://bucket-beam-simon/input");
		myOption.setProject("beam-test-296614");  
		
    Pipeline p = Pipeline.create(myOption);
    
    List<TableFieldSchema> columns = new ArrayList<TableFieldSchema>();
    columns.add(new TableFieldSchema().setName("userId").setType("STRING"));
    columns.add(new TableFieldSchema().setName("orderId").setType("STRING"));
    columns.add(new TableFieldSchema().setName("name").setType("STRING"));
    columns.add(new TableFieldSchema().setName("productId").setType("STRING"));
    columns.add(new TableFieldSchema().setName("Amount").setType("INTEGER"));
    columns.add(new TableFieldSchema().setName("order_date").setType("STRING"));    
    columns.add(new TableFieldSchema().setName("country").setType("STRING"));     
    
    TableSchema tblSchema = new TableSchema().setFields(columns);
    
    PCollection<String> pInput = p.apply(TextIO.read().from("gs://bucket-beam-simon/input/user1.csv"));

    //this will alows to show all register that are US and also arr.length==7
    pInput.apply(ParDo.of(new DoFn<String,TableRow>(){
    	
    	@ProcessElement
    	public void processElement(ProcessContext c) {
    		String arr[]=c.element().split(",");
    		
    		//a valid arr contains length==7
    		if (arr.length==7) {
    			if(arr[6].equalsIgnoreCase("US")) {
    				
    				TableRow row = new TableRow();
    				
    				row.set("userId", arr[0]);
    				row.set("orderId", arr[1]);
    				row.set("name", arr[2]);
    				row.set("productId", arr[3]);
    				row.set("Amount", Integer.valueOf(arr[4]));
    				row.set("order_date", arr[5]);
    				row.set("country", arr[6]);    				
    				c.output(row);    
    				
    				//System.out.println(c.element());
    			}
    			
    		}
    		
    	}
    	
    }))
    .apply(BigQueryIO.writeTableRows().to("dataset_test.user_tbl")
			.withSchema(tblSchema)
			.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            );
    
    p.run();
  }
}
