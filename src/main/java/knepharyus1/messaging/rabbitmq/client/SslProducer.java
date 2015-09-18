package knepharyus1.messaging.rabbitmq.client;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.ArrayUtils;

import knepharyus1.messaging.rabbitmq.handler.SslMessageHandler;

public class SslProducer {

  public static void main(String[] args) throws IOException,
  KeyManagementException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException,
  CertificateException, InterruptedException, ConfigurationException {

    SslMessageHandler handler;
    String opt = args[0];
    String[] arguments = (String[]) ArrayUtils.remove(args, 0);

    File configFile = new File(opt);

    if (configFile.exists()) {
      handler = new SslMessageHandler(opt);
      arguments = (String[]) ArrayUtils.remove(args, 0);
    } else {
      handler = new SslMessageHandler();
    }

    if ("p".equals(opt)) {
      // publish
      for (String key : arguments) {
        handler.publish("Publishing with the '" + key + "' key...", key);
      }

    } else if ("pwp".equals(opt)) {
      // publish with properties
      Map<String, Object> props = new HashMap<String, Object>();
      props.put("source_exchange", "knepharyus1.test.headers");
      props.put("destination_system", "knepharyus1.server");
      props.put("destination_vhost", "knepharyus1.vhost");
      props.put("destination_exchange", "knepharyus1.test.headers");

      for (String routeSig : arguments) {
        props.put("route_signature", routeSig);
        handler.publishWithProperties("publishWithProperties...", props, "");
      }

    } else if ("p2q".equals(opt)) {
      // publish to queue
      for (String queue : arguments) {
        handler.publishToQueue("Publishing straight to queue", queue);
      }

    } else if ("flood".equals(opt)) {
      // flood the queue
      int count = Integer.parseInt(arguments[0]);
      arguments = (String[]) ArrayUtils.remove(arguments, 0);
      for (String queue : arguments) {
        for (int i = 0; i < count; i++) {
          handler.publishToQueue("Flooding queue " + queue + "... " + i, queue);
        }
      }

    } else if ("flooddelay".equals(opt)) {
      // flood the queue with delay between each new publish
      int count = Integer.parseInt(arguments[0]);
      arguments = (String[]) ArrayUtils.remove(arguments, 0);
      int delay = Integer.parseInt(arguments[0]);
      arguments = (String[]) ArrayUtils.remove(arguments, 0);
      for (String queue : arguments) {
        for (int i = 0; i < count; i++) {
          handler.publishToQueue("Flooding queue " + queue + "... " + i, queue);
          Thread.sleep(delay * 100);
        }
      }
    }

    handler.close();

  }

}
