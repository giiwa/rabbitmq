package org.giiwa.rabbitmq.mq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.giiwa.core.bean.TimeStamp;
import org.giiwa.core.bean.X;
import org.giiwa.core.conf.Global;
import org.giiwa.core.json.JSON;
import org.giiwa.core.task.Task;
import org.giiwa.framework.bean.OpLog;
import org.giiwa.framework.bean.Request;
import org.giiwa.framework.bean.Response;
import org.giiwa.rabbitmq.web.admin.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * the distribute message system, <br>
 * the performance: sending 1w/300ms <br>
 * recving 1w/1500ms<br>
 * 
 * @author joe
 *
 */
public final class MQ {

  private static Log log = LogFactory.getLog(MQ.class);

  /**
   * the message stub type <br>
   * TOPIC: all stub will read it <br>
   * QUEUE: only one will read it
   * 
   * @author joe
   *
   */
  public static enum Mode {
    TOPIC, QUEUE
  };

  private static String     url;       // failover:(tcp://localhost:61616,tcp://remotehost:61616)?initialReconnectDelay=100
  private static Channel    channel;
  private static Connection connection;

  public static boolean init() {
    if (connection == null) {
      try {
        url = Global.getString("rabbitmq.url", X.EMPTY);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(url);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(10000);

        ExecutorService es = Executors.newFixedThreadPool(20);

        connection = factory.newConnection(es);
        channel = connection.createChannel();

        OpLog.info(rabbitmq.class, "startup", "connected RabbitMQ with [" + url + "]", null, null);

      } catch (Throwable e) {
        log.error(e.getMessage(), e);
        OpLog.warn(rabbitmq.class, "startup", "failed RabbitMQ with [" + url + "]", null, null);
      }
    }

    return connection != null;
  }

  private MQ() {
  }

  /**
   * listen on the name
   * 
   * @param name
   * @param stub
   * @throws JMSException
   */
  public static Receiver bind(String name, IStub stub, Mode mode) {

    Receiver r = null;
    try {
      r = new Receiver(name, stub, mode);
      OpLog.info(rabbitmq.class, "bind", "[" + name + "], stub=" + stub.getClass().toString() + ", mode=" + mode, null,
          null);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      OpLog.warn(rabbitmq.class, "bind",
          "[" + name + "] failed, error=" + e.getMessage() + ", stub=" + stub.getClass().toString() + ", mode=" + mode,
          null, null);
    }
    return r;
  }

  public static Receiver bind(String name, IStub stub) {
    return bind(name, stub, Mode.QUEUE);
  }

  /**
   * QueueTask
   * 
   * @author joe
   * 
   */
  public final static class Receiver extends DefaultConsumer {
    String    name;
    IStub     cb;
    TimeStamp t     = TimeStamp.create();
    int       count = 0;

    public void close() {
      // TODO

    }

    private Receiver(String name, IStub cb, Mode mode) {
      super(channel);

      this.cb = cb;

      if (connection != null) {
        try {

          channel.queueDeclare(name, false, false, false, null);
          channel.basicConsume(name, true, this);

        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }

      } else {
        log.warn("no mq configured!");
      }
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
        throws IOException {

      Request req = new Request(body, 0);

      count++;

      process(name, req, cb);

      if (count % 10000 == 0) {
        System.out.println("process the 10000 messages, cost " + t.reset() + "ms");
      }

    }
  }

  private static void process(final String name, final Request req, final IStub cb) {

    new Task() {
      @Override
      public void onExecute() {

        try {

          long seq = req.readLong();
          String to = req.readString();
          String from = req.readString();

          long time = req.readLong();
          long delay = System.currentTimeMillis() - time;
          if (delay > 1000) {
            log.warn("MQ[" + name + "] reader delayed " + delay + "ms");
          }

          JSON message = null;
          int len = req.readInt();
          if (len > 0) {
            byte[] bb = req.readBytes(len);
            ByteArrayInputStream is = new ByteArrayInputStream(bb);
            ObjectInputStream in = new ObjectInputStream(is);
            message = (JSON) in.readObject();
            in.close();
          }

          byte[] bb = null;
          len = req.readInt();
          if (len > 0) {
            bb = req.readBytes(len);
          }

          log.debug("got a message:" + from + ", " + message);

          cb.onRequest(seq, to, from, message, bb);

        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
      }
    }.schedule(0);

  }

  /**
   * send the message and return immediately
   * 
   * @param seq
   * @param to
   * @param message
   * @param bb
   * @param from
   * @return int 1: success
   */
  public static int send(long seq, String to, JSON msg, byte[] bb, String from) {
    if (msg == null)
      return -1;

    if (connection == null) {
      return -1;
    }

    try {

      /**
       * get the message producer by destination name
       */
      if (channel != null) {
        Response resp = new Response();

        // Response resp = new Response();
        resp.writeLong(seq);
        resp.writeString(to == null ? X.EMPTY : to);
        resp.writeString(from == null ? X.EMPTY : from);
        resp.writeLong(System.currentTimeMillis());

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(os);
        out.writeObject(msg);
        out.close();
        byte[] ss = os.toByteArray();
        resp.writeInt(ss.length);
        resp.writeBytes(os.toByteArray());

        if (bb == null) {
          resp.writeInt(0);
        } else {
          resp.writeInt(bb.length);
          resp.writeBytes(bb);
        }

        channel.queueDeclare(to, false, false, false, null);
        channel.basicPublish("", to, null, resp.getBytes());

        log.debug("Sending:" + to + ", " + msg);

        return 1;
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }

    return -1;
  }

}
