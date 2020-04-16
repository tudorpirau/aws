package demo;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.gson.Gson;
import com.opencsv.CSVReader;

import demo.Response;

public class saveUser implements RequestHandler<SQSEvent, Response> {
   
  private DynamoDB dynamoDb;
  private String DYNAMODB_TABLE_NAME = "users";
  private final String REGION = "eu-central-1";
  public final String statusOk="Inserted.OK!";
  public final String statusError="Insert failed!";
  public Response handleRequest(SQSEvent event, Context context) {

      this.initDynamoDbClient();
      Response response=new Response();
      for(SQSMessage msg : event.getRecords()){
          String json=msg.getBody();
          Gson gson = new Gson();
          User user= (User) gson.fromJson(json, User.class);
          AmazonS3ClientBuilder s3Client=AmazonS3ClientBuilder.standard();
          final AmazonS3 s3 = s3Client.withRegion(Regions.EU_CENTRAL_1).build();
          List<Bucket> buckets = s3.listBuckets();
          for (Bucket b : buckets) {
        	  ListObjectsV2Result result = s3.listObjectsV2(b.getName());
        	  List<S3ObjectSummary> objects = result.getObjectSummaries();
        	  for (S3ObjectSummary os: objects) {
        		  S3Object object = s3.getObject(b.getName(), os.getKey());
        		  InputStream objectData = object.getObjectContent();
        		  try {
        			CSVReader reader=new CSVReader(new InputStreamReader(objectData));
        			List<String[]> rows=reader.readAll();
        			for (String[] row: rows) {
        				if (Integer.parseInt(row[0])==user.getId()) {
        					user.setName(row[1]);
        				}
        			}
        			s3.deleteObject(b.getName(), os.getKey());
					objectData.close();
        		  	} catch (IOException e) {
						
						e.printStackTrace();
					} 
        	  }
          }
          
	      if (persistData(user).getPutItemResult()!=null) {
	    	  
	    	  response.setMessage(statusOk);
	    	  
	      } else {
	    	  response.setMessage(statusError);
	    	  
	      }
      }
      return response;
  }

  private PutItemOutcome persistData(User user) 
    throws ConditionalCheckFailedException {
      return this.dynamoDb.getTable(DYNAMODB_TABLE_NAME)
        .putItem(
          new PutItemSpec().withItem(new Item()
            .withNumber("id", user.getId())
            .withString("name", user.getName())));
  }

  private void initDynamoDbClient() {
	  AmazonDynamoDBClientBuilder client= AmazonDynamoDBClientBuilder.standard();
	  client.setRegion(REGION);
      this.dynamoDb = new DynamoDB(client.build());
      
  }
}
