package org.drools.elasticsearch;

import static com.jayway.restassured.RestAssured.given;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.jayway.restassured.response.Response;

public class ElasticSearchManagement {
	public static final String elasticSearchServer="http://localhost:9200";
	
	public static void main(String[] args){
//		new ElasticSearchManagement().deleteIndex("mallen");
//		new ElasticSearchManagement().deleteIndex("logstash-2013.10.13");
		Date now=new Date(System.currentTimeMillis());
		new SimpleDateFormat("yyyy'.'MM'.'dd").format(now);
		
		new ElasticSearchManagement().deleteIndex("drools-2013.10.24");
		new ElasticSearchManagement().deleteIndex("2013.10.25");
		
	}
	
	public void deleteIndex(String indexName){
//		curl -XDELETE http://localhost:9200/logstash-2013.08.13/
		
		Response response = given()
				.redirects()
				.follow(true)
				.when()
				.delete(elasticSearchServer+"/"+indexName);
		System.out.println(response.statusLine() +" :: "+response.asString());
	}
	
}
