package org.drools.elasticsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;

import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseConfiguration;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.conf.EventProcessingOption;
import org.drools.definition.type.FactType;
import org.drools.event.rule.AgendaEventListener;
import org.drools.event.rule.WorkingMemoryEventListener;
import org.drools.eventlistener.elasticsearch.ElasticSearchEventListener;
import org.drools.logger.KnowledgeRuntimeLogger;
import org.drools.runtime.StatefulKnowledgeSession;
import org.mvel2.optimizers.impl.refl.nodes.ArrayLength;

public class TestElasticSearchLogging {

	public static void main(String[] arg) {
//		new TestElasticSearchLogging().run();
		new TestElasticSearchLogging().threadedRun(2, 50);
	}
	
	
	public void threadedRun(int threads, int iterations){
		List<FutureTask<Void>> tasks=new ArrayList<FutureTask<Void>>();
		ExecutorService executor = Executors.newFixedThreadPool(threads);
		
		for(int i=0;i<iterations;i++){
			tasks.add(new FutureTask<Void>(new Callable<Void>() {
				public Void call() throws Exception {
					new TestElasticSearchLogging().run();
					try {
						Generator<Long> generator = net.java.quickcheck.generator.PrimitiveGenerators.longs(300, 7000);
						long wait = generator.next().longValue();
						System.out.println("WAIT = "+ wait);
						Thread.sleep(wait);
					} catch (Exception sink) {
						sink.printStackTrace();
					}
					return null;
				}
			}));
		}
		
		for(FutureTask<Void> t:tasks){
			executor.submit(t);
		}
		
		for(FutureTask<Void> t:tasks){
			while (!t.isDone()){
				try {
					Thread.sleep(1000l);
				} catch (InterruptedException sink) {}
			}
			System.out.println("Task done.");
		}
		executor.shutdown();
	}
	

	public void run() {
		KnowledgeBuilder builder = KnowledgeBuilderFactory.newKnowledgeBuilder();

		builder.add(org.drools.io.ResourceFactory.newFileResource("src/main/resources/test.drl"), ResourceType.DRL);

		if (builder.hasErrors())
			throw new RuntimeException(builder.getErrors().toString());
		KnowledgeBaseConfiguration configuration = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
		configuration.setOption(EventProcessingOption.STREAM);

		KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase(configuration);
		kbase.addKnowledgePackages(builder.getKnowledgePackages());
//		el.markEndOfKnowledgeBaseCreation();
		
		StatefulKnowledgeSession session = kbase.newStatefulKnowledgeSession();
//		el.markEndOfSessionCreation();
		KnowledgeRuntimeLogger logger = org.drools.logger.KnowledgeRuntimeLoggerFactory.newFileLogger(session, "/tmp/drools.log");
		// KnowledgeRuntimeLogger
		// logger=org.drools.logger.KnowledgeRuntimeLoggerFactory.newConsoleLogger(session);//,
		// "/tmp/drools.log");
		ElasticSearchEventListener el=new ElasticSearchEventListener();
		session.addEventListener((WorkingMemoryEventListener)el);
		session.addEventListener((AgendaEventListener)el);
		
		try {
			el.markStartOfFactInsertion();
			FactType factType = kbase.getFactType("testpackage", "MyFact");
			Object fact = factType.newInstance();
			factType.set(fact, "name", "fred");
			factType.set(fact, "amount", PrimitiveGenerators.integers(1, 99).next().intValue());
			factType.set(fact, "approved", Boolean.TRUE);

			session.insert(new String("test"));
			session.insert(fact);
			
			el.markEndOfFactInsertion();
			
			el.markStartOfRuleFiring();
			session.fireAllRules();
			el.markEndOfRuleFiring();
			
			el.sendSummaryToElasticSearchServer("http://localhost:9200", "executions", "<breadcrumb>", session);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			session.removeEventListener((WorkingMemoryEventListener)el);
			session.removeEventListener((AgendaEventListener)el);
			session.dispose();
		}
	}
}
