package knepharyus1.messaging.rabbitmq.handler;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.BasicProperties.Builder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultSaslConfig;
import com.rabbitmq.client.QueueingConsumer;

public class SslMessageHandler {

	private static final String CONFIGPATH = "META-INF/MessageHandlerConfig.xml";

	private String host;
	private String exchange;
	private Connection conn;
	private Channel channel;
	private QueueingConsumer consumer;
	private ConnectionFactory factory;

	public SslMessageHandler() throws IOException, KeyManagementException,
	NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, CertificateException, ConfigurationException {
		this(CONFIGPATH);
	}

	public SslMessageHandler(String configFile) throws IOException, KeyManagementException,
	NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, CertificateException, ConfigurationException {
		super();

		char[] keyPass;
		char[] trustPass;
		String keyStoreFile;
		String trustStoreFile;
		String vhost = null;

		XMLConfiguration xmlConf = new XMLConfiguration();
		xmlConf.load(configFile);

		host = xmlConf.getProperty("host").toString();
		exchange = xmlConf.getProperty("exchange").toString();
		keyPass = xmlConf.getProperty("keyPass").toString().toCharArray();
		trustPass = xmlConf.getProperty("trustPass").toString().toCharArray();
		keyStoreFile = xmlConf.getProperty("keyStoreFile").toString();
		trustStoreFile = xmlConf.getProperty("trustStoreFile").toString();

		if (xmlConf.getProperty("vhost") != null) {
			vhost = xmlConf.getProperty("vhost").toString();
		}

		Integer port = Integer.parseInt(xmlConf.getProperty("sslPort").toString());

		KeyStore keyStore = KeyStore.getInstance("PKCS12");
		keyStore.load(new FileInputStream(keyStoreFile), keyPass);

		KeyManagerFactory kmFactory = KeyManagerFactory.getInstance("SunX509");
		kmFactory.init(keyStore, keyPass);

		KeyStore trustStore = KeyStore.getInstance("JKS");
		trustStore.load(new FileInputStream(trustStoreFile), trustPass);

		TrustManagerFactory tmFactory = TrustManagerFactory.getInstance("SunX509");
		tmFactory.init(trustStore);

		SSLContext c = SSLContext.getInstance("SSLv3");
		c.init(kmFactory.getKeyManagers(), tmFactory.getTrustManagers(), null);

		factory = new ConnectionFactory();
		factory.setHost(host);
		factory.setPort(port);
		if (vhost != null) {
			factory.setVirtualHost(vhost);
		}
		factory.setSaslConfig(DefaultSaslConfig.EXTERNAL);
		factory.useSslProtocol(c);

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

		this.basicPublish(exchange, routingKey, basicProps, message.getBytes());
	}

	public void publishWithProperties(String message, Map<String, Object> props) {
		Builder builder = new Builder();
		builder.headers(props);
		BasicProperties basicProps = builder.build();

		this.basicPublish(exchange, null, basicProps, message.getBytes());
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

	private void basicConsume(String queue) throws IOException {
		this.newChannel();
		consumer = new QueueingConsumer(channel);
		channel.basicConsume(queue, true, consumer);
	}

}
