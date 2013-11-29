package org.drools.eventlistener.elasticsearch;

import static com.jayway.restassured.RestAssured.given;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.drools.elasticsearch.FreemarkerHelper;
import org.drools.event.rule.ActivationCancelledEvent;
import org.drools.event.rule.ActivationCreatedEvent;
import org.drools.event.rule.AfterActivationFiredEvent;
import org.drools.event.rule.AgendaEventListener;
import org.drools.event.rule.AgendaGroupPoppedEvent;
import org.drools.event.rule.AgendaGroupPushedEvent;
import org.drools.event.rule.BeforeActivationFiredEvent;
import org.drools.event.rule.ObjectInsertedEvent;
import org.drools.event.rule.ObjectRetractedEvent;
import org.drools.event.rule.ObjectUpdatedEvent;
import org.drools.event.rule.RuleFlowGroupActivatedEvent;
import org.drools.event.rule.RuleFlowGroupDeactivatedEvent;
import org.drools.event.rule.WorkingMemoryEventListener;
import org.drools.runtime.StatefulKnowledgeSession;

import com.jayway.restassured.response.Response;

public class ElasticSearchEventListener implements WorkingMemoryEventListener, AgendaEventListener{
	private List<String> rulesFired=new ArrayList<String>();
	public List<String> getRulesFired(){
		return rulesFired;
	}
	Long factInsertionDuration=null;
	public void markStartOfFactInsertion(){
		factInsertionDuration=System.currentTimeMillis();	
	}
	public void markEndOfFactInsertion(){
		factInsertionDuration=System.currentTimeMillis()-factInsertionDuration;
	}
	Long ruleFiringDuration=null;
	public void markStartOfRuleFiring(){
		ruleFiringDuration=System.currentTimeMillis();	
	}
	public void markEndOfRuleFiring(){
		ruleFiringDuration=System.currentTimeMillis()-ruleFiringDuration;
	}
	
	// WorkingMemoryEventListener
	public void objectInserted(ObjectInsertedEvent arg0) {}
	public void objectRetracted(ObjectRetractedEvent arg0) {}
	public void objectUpdated(ObjectUpdatedEvent arg0) {}
	public void activationCancelled(ActivationCancelledEvent arg0) {}
	public void activationCreated(ActivationCreatedEvent arg0) {}
	public void afterActivationFired(AfterActivationFiredEvent arg0) {
//		rulesFired.add((rulesFired.size()+1) +" - "+arg0.getActivation().getRule().getName());
		rulesFired.add(arg0.getActivation().getRule().getName());
	}
	public void afterRuleFlowGroupActivated(RuleFlowGroupActivatedEvent arg0) {}
	public void afterRuleFlowGroupDeactivated(RuleFlowGroupDeactivatedEvent arg0) {}
	public void agendaGroupPopped(AgendaGroupPoppedEvent arg0) {}
	public void agendaGroupPushed(AgendaGroupPushedEvent arg0) {}
	public void beforeActivationFired(BeforeActivationFiredEvent arg0) {}
	public void beforeRuleFlowGroupActivated(RuleFlowGroupActivatedEvent arg0) {}
	public void beforeRuleFlowGroupDeactivated(RuleFlowGroupDeactivatedEvent arg0) {}
	
	public void sendSummaryToElasticSearchServer(String elasticSearchServer, String type, String breadcrumb, StatefulKnowledgeSession session){
		Map<String,Object> replacements=new HashMap<String,Object>();
		String executionId=String.valueOf(System.currentTimeMillis());
		replacements.put("ID", executionId);
		
		Date now=new Date(System.currentTimeMillis());
		replacements.put("DATE", new SimpleDateFormat("yyyy'.'MM'.'dd").format(now));
		replacements.put("TIMESTAMP", new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH:MM:ss").format(now));
		replacements.put("BREADCRUMB", breadcrumb);
		replacements.put("MESSAGE", rulesFired.size()+" rule(s) fired");
		replacements.put("FACTSSIZE", session.getFactCount());
		replacements.put("RULESSIZE", rulesFired.size());
		replacements.put("RULESFIRED", rulesFired);
		replacements.put("TYPE", type);
		replacements.put("FACT_INSERTION_DURATION", factInsertionDuration!=null?String.valueOf(factInsertionDuration):"-1");
		replacements.put("RULE_FIRING_DURATION", ruleFiringDuration!=null?String.valueOf(ruleFiringDuration):"-1");
		
		String indexName="logstash-"+replacements.get("DATE");
		replacements.put("INDEX", indexName);
		
		String body=FreemarkerHelper.parse("elasticsearch-template.json", replacements);
		Response response = given()
				.redirects()
				.follow(true)
				.body(body)
				.when()
				.put(elasticSearchServer+"/"+indexName+"/"+type+"/"+executionId);
		
		System.out.println("PUT: "+elasticSearchServer+"/"+indexName+"/"+type+"/"+executionId);
		System.out.println(body);
		System.out.println(response.statusLine() +" :: "+response.asString());
	}
}
