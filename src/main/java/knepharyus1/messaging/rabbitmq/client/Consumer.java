package knepharyus1.messaging.rabbitmq.client;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.configuration.ConfigurationException;

import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import knepharyus1.messaging.rabbitmq.handler.MessageHandler;

public class Consumer {
	
	MessageHandler handler;
	
	public Consumer(String config) throws ConfigurationException, IOException, 
	TimeoutException {
		super();
		
		File configFile = new File(config);

    if (configFile.exists()) {
      handler = new MessageHandler(config);
    } else {
      handler = new MessageHandler();
    }
    
	}
	
	public Consumer(String host, String exchange, String username, String vhost) {
		handler = new MessageHandler(host, exchange, username, vhost);
	}
	
	public void consumeWithDelay(String queue, int delay) throws IOException, InterruptedException {
		
		try {

//			while(true) {
//	      System.out.println("Consumed: " + handler.getChannel().basicGet(queue, true).toString());
//	      Thread.sleep(delay * 100);
//			}
			
      handler.consume(queue);

      System.out.println(" [*] Waiting for messages SSL. To exit press Ctrl+C");

      int count = 0;
      QueueingConsumer.Delivery delivery = null;
      
      
      while (true) {
        try {
          delivery = handler.getConsumer().nextDelivery();
        } catch (Exception e) {
          if (e instanceof ShutdownSignalException) {
            System.out.println("ERROR: Message not delivered; Shutdown signal "
            	+ "exception thrown, retrying...");
            handler.consume(queue);
          } else {
            System.out.println("Exception...");
            e.printStackTrace();
            handler.close();
          }
        }

        String message = new String(delivery.getBody());
        String key = delivery.getEnvelope().getRoutingKey();
        System.out.println(" [x] Received '" + key + "':'" + message + "' TOTAL: " + ++count);
        
        Thread.sleep(delay * 100);
        
      }

    } catch (IOException iox) {
      handler.close();
    }
		
	}
	 public void consume(String queue) throws IOException {
		 try {

	      handler.consume(queue);

	      System.out.println(" [*] Waiting for messages SSL. To exit press Ctrl+C");

	      int count = 0;
	      QueueingConsumer.Delivery delivery = null;
	      while (true) {
	        try {
	          delivery = handler.getConsumer().nextDelivery();
	        } catch (Exception e) {
	          if (e instanceof ShutdownSignalException) {
	            System.out.println("ERROR: Message not delivered; Shutdown signal "
	            	+ "exception thrown, retrying...");
	            handler.consume(queue);
	          } else {
	            System.out.println("Exception...");
	            e.printStackTrace();
	            handler.close();
	          }
	        }

	        String message = new String(delivery.getBody());
	        String key = delivery.getEnvelope().getRoutingKey();
	        System.out.println(" [x] Received '" + key + "':'" + message + "' TOTAL: " + ++count);
	      }

	    } catch (IOException iox) {

	      if (iox.getCause() instanceof ShutdownSignalException) {

	      } else {

	      }

	      handler.close();
	    }
	 }
	 
	 
//	public void consume(String config, String queue) throws ConfigurationException, 
//	IOException, TimeoutException {
//		MessageHandler handler;
//		
//		File configFile = new File(config);
//		
//		if (configFile.exists()) {
//      handler = new MessageHandler(config);
//    } else {
//      handler = new MessageHandler();
//    }
//		
//		try {
//
//      handler.consume(queue);
//
//      System.out.println(" [*] Waiting for messages SSL. To exit press Ctrl+C");
//
//      int count = 0;
//      QueueingConsumer.Delivery delivery = null;
//      while (true) {
//        try {
//          delivery = handler.getConsumer().nextDelivery();
//        } catch (Exception e) {
//          if (e instanceof ShutdownSignalException) {
//            System.out.println("ERROR: Message not delivered; Shutdown signal "
//            	+ "exception thrown, retrying...");
//            handler.consume(queue);
//          } else {
//            System.out.println("Exception...");
//            e.printStackTrace();
//            handler.close();
//          }
//        }
//
//        String message = new String(delivery.getBody());
//        String key = delivery.getEnvelope().getRoutingKey();
//        System.out.println(" [x] Received '" + key + "':'" + message + "' TOTAL: " + ++count);
//      }
//
//    } catch (IOException iox) {
//
//      if (iox.getCause() instanceof ShutdownSignalException) {
//
//      } else {
//
//      }
//
//      handler.close();
//    }
//
//	}
//
//  public static void main(String[] args)
//  throws ConfigurationException, IOException, ConsumerCancelledException, InterruptedException,
//  KeyManagementException, UnrecoverableKeyException, NoSuchAlgorithmException, 
//  KeyStoreException, CertificateException {
//
//    // MessageHandler handler = new MessageHandler();
//    SslMessageHandler handler;
//    String[] arguments = args;
//    String opt = args[0];
//
//    File configFile = new File(opt);
//
//    if (configFile.exists()) {
//      handler = new SslMessageHandler(opt);
//      arguments = (String[]) ArrayUtils.remove(args, 0);
//    } else {
//      handler = new SslMessageHandler();
//    }
//
//    try {
//
//      handler.consume(arguments[0]);
//
//      System.out.println(" [*] Waiting for messages SSL. To exit press Ctrl+C");
//
//      int count = 0;
//      QueueingConsumer.Delivery delivery = null;
//      while (true) {
//        try {
//          delivery = handler.getConsumer().nextDelivery();
//        } catch (Exception e) {
//          if (e instanceof ShutdownSignalException) {
//            System.out.println("ERROR: Message not delivered; Shutdown signal exception thrown, retrying...");
//            handler.consume(arguments[0]);
//          } else {
//            System.out.println("Exception...");
//            e.printStackTrace();
//            handler.close();
//          }
//        }
//
//        String message = new String(delivery.getBody());
//        String key = delivery.getEnvelope().getRoutingKey();
//        System.out.println(" [x] Received '" + key + "':'" + message + "' TOTAL: " + ++count);
//      }
//
//    } catch (IOException iox) {
//
//      if (iox.getCause() instanceof ShutdownSignalException) {
//
//      } else {
//
//      }
//
//      handler.close();
//    }
//  }
  
}
