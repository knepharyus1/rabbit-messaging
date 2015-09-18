package knepharyus1.messaging.rabbitmq.handler;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
//import org.apache.log4j.Logger;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.BasicProperties.Builder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class MessageHandler {

  private static final String CONFIGPATH = "META-INF/MessageHandlerConfig.xml";
//  private static Logger logger = Logger.getLogger(MessageHandler.class);

  private String host;
  private String exchange;
  private String username;
  private Connection conn;
  private Channel channel;
  private QueueingConsumer consumer;
  private ConnectionFactory factory;

  public MessageHandler() throws IOException, TimeoutException, ConfigurationException {
    this(CONFIGPATH);
  }

  public MessageHandler(String configFile) throws IOException, TimeoutException, ConfigurationException {
    super();
    
    String vhost = null;

    XMLConfiguration xmlConf = new XMLConfiguration();
    xmlConf.load(configFile);

    host = xmlConf.getProperty("host").toString();
    exchange = xmlConf.getProperty("exchange").toString();
    username = xmlConf.getProperty("username").toString();

    if (xmlConf.getProperty("vhost") != null) {
      vhost = xmlConf.getProperty("vhost").toString();
    }

    System.out.println("host: " + host + "; exchange: " + exchange + "; vhost: " + (vhost == null ? "null" : vhost));

    factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(5672);
    factory.setUsername(username);
    factory.setPassword(username);
    if (vhost != null) {
      factory.setVirtualHost(vhost);
    }

//    conn = factory.newConnection();
//    channel = conn.createChannel();
    this.newChannel();

  }

  private void newChannel() {
    channel = null;
    while (channel == null) {
      try {
        Thread.sleep(200);
        conn = factory.newConnection();
        channel = conn.createChannel();
      } catch (Exception e) {
      	e.printStackTrace();
        System.err.println("connection to the broker failed, retrying...");
      }
    }
  }


  public void publish(String message, String routingKey) {
    this.basicPublish(exchange, routingKey, null, message.getBytes());
  }
  
  public void publishToQueue(String message, String queue) {
  	this.basicPublish("", queue, null, message.getBytes());
  }

  public void publishWithProperties(String message, Map<String, Object> props, String routingKey) {
    Builder builder = new Builder();
    builder.headers(props);
    BasicProperties basicProps = builder.build();
//    for (String key : basicProps.getHeaders().keySet()) {
//
//    }
    this.basicPublish(exchange, routingKey, basicProps, message.getBytes());
  }
  
  public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body) {
    boolean success = false;
    while (!success) {
      try {
        channel.basicPublish(exchange, routingKey, props, body);
        success = true;
      } catch (Exception e) {
        System.err.println("publishToQueue failed, retrying...");
        this.newChannel();
      }
    }
  }

  public void consume(String queue) throws IOException {
    this.basicConsume(queue);
  }

  public void close() throws IOException {
    conn.close();
  }

  public QueueingConsumer getConsumer() {
    return consumer;
  }

  public void basicConsume(String queue) throws IOException {
    consumer = new QueueingConsumer(channel);
    channel.basicConsume(queue, true, consumer);
  }

}
