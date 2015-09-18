package knepharyus1.messaging.rabbitmq.client;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.configuration.ConfigurationException;

import knepharyus1.messaging.rabbitmq.handler.MessageHandler;

public class Producer {
	
	MessageHandler handler;
	
	public Producer(String config) throws ConfigurationException, IOException, 
	TimeoutException {
		super();
		
		File configFile = new File(config);

    if (configFile.exists()) {
      handler = new MessageHandler(config);
    } else {
      handler = new MessageHandler();
    }
		
	}
	
	public void publish(String[] keys) throws IOException {
		
		for (String key : keys) {
			System.out.println("Publishing with the '" + key + "' key...");
			handler.publish("Publishing with the '" + key + "' key...", key);
		}
		
		handler.close();
		
	}
	
	public void publishToQueue(String[] queues) throws IOException {
		
		for (String queue : queues) {
			System.out.println("Publishing straight to queue");
			handler.publishToQueue("Publishing straight to queue", queue);
		}
		
		handler.close();
		
	}
	
	public void floodQueue(String queue, int count) throws IOException {
		
    for (int i = 0; i < count; i++) {
    	System.out.println("Flooding queue " + queue + "... " + i);
      handler.publishToQueue("Flooding queue " + queue + "... " + i, queue);
    }
    
    handler.close();
    
	}
	
	public void floodQueueWithDelay(String queue, int count, int delay) throws 
	InterruptedException, IOException {
		
    for (int i = 0; i < count; i++) {
    	System.out.println("Flooding queue " + queue + "... " + i);
      handler.publishToQueue("Flooding queue " + queue + "... " + i, queue);
      Thread.sleep(delay * 100);
    }
		
		handler.close();
		
	}


}
