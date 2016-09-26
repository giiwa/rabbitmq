package org.giiwa.rabbitmq.mq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.giiwa.core.bean.TimeStamp;
import org.giiwa.core.bean.X;
import org.giiwa.core.json.JSON;
import org.giiwa.core.task.Task;
import org.giiwa.framework.bean.OpLog;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * the distribute message system, <br>
 * the performance: sending 1w/300ms <br>
 * recving 1w/1500ms<br>
 * 
 * @author joe
 *
 */
public final class MQ {

  private static String group = X.EMPTY;

  private static Log    log   = LogFactory.getLog(MQ.class);

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

  private static boolean                   enabled = false;
  private static String                    url;            // failover:(tcp://localhost:61616,tcp://remotehost:61616)?initialReconnectDelay=100
  private static String                    user;
  private static String                    password;
  private static Channel                   channel;
  private static Connection                connection;

  private static boolean init() {
    if (enabled) {
      try {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");
        connection = factory.newConnection();
        channel = connection.createChannel();

        // OpLog.info(activemq.class, "startup", "connected ActiveMQ with [" +
        // url + "]", null, null);

      } catch (Throwable e) {
        log.error(e.getMessage(), e);
        // e.printStackTrace();
        // OpLog.info(activemq.class, "startup", "failed ActiveMQ with [" + url
        // + "]", null, null);
      }
    }

    return enabled && channel != null;
  }

  private MQ() {
  }

  /**
   * initialize the MQ
   * 
   * @param conf
   * @return boolean
   */
  public static boolean init(Configuration conf) {
    if (session != null)
      return true;

    enabled = true;

    url = conf.getString("activemq.url", ActiveMQConnection.DEFAULT_BROKER_URL);
    user = conf.getString("activemq.user", ActiveMQConnection.DEFAULT_USER);
    password = conf.getString("activemq.passwd", ActiveMQConnection.DEFAULT_PASSWORD);

    group = conf.getString("activemq.group", X.EMPTY);
    if (!group.endsWith(".")) {
      group += ".";
    }

    // OpLog.info(activemq.class, "startup", url + ", " + group, null, null);

    return init();

  }

  /**
   * listen on the name
   * 
   * @param name
   * @param stub
   * @throws JMSException
   */
  public static Receiver bind(String name, IStub stub, Mode mode) {
    OpLog.info(rabbitmq.class, "bind", "[" + name + "], stub=" + stub.getClass().toString() + ", mode=" + mode, null,
        null);

    return new Receiver(name, stub, mode);
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
  public final static class Receiver implements Consumer {
    String          name;
    IStub           cb;
    MessageConsumer consumer;
    TimeStamp       t     = TimeStamp.create();
    int             count = 0;

    public void close() {
      if (consumer != null) {
        this.channel.close();
        this.connection.close();

        try {
          consumer.close();
        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
      }
    }

    private Receiver(String name, IStub cb, Mode mode) {
      this.name = group + name;
      this.cb = cb;

      if (enabled) {

        channel.queueDeclare(name, true, false, false, null);
        channel.basicConsume(name, true, this);

        Destination dest = null;
        if (mode == Mode.QUEUE) {
          dest = new ActiveMQQueue(group + name);
        } else {
          dest = new ActiveMQTopic(group + name);
        }

        consumer = session.createConsumer(dest);
        consumer.setMessageListener(this);

      } else {
        log.warn("no mq configured!");
      }
    }
    
    @Override
    public void onMessage(Message m) {
      try {
        // System.out.println("got a message.., " + t.reset() +
        // "ms");

        count++;
        if (m instanceof BytesMessage) {
          BytesMessage m1 = (BytesMessage) m;
          process(name, m1, cb);
        } else {
          System.out.println(m);
        }

        if (count % 10000 == 0) {
          System.out.println("process the 10000 messages, cost " + t.reset() + "ms");
        }

      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    }

    @Override
    public void handleCancel(String arg0) throws IOException {
      // TODO Auto-generated method stub

    }

    @Override
    public void handleCancelOk(String arg0) {
      // TODO Auto-generated method stub

    }

    @Override
    public void handleConsumeOk(String arg0) {
      // TODO Auto-generated method stub

    }

    @Override
    public void handleDelivery(String arg0, Envelope arg1, BasicProperties arg2, byte[] bb) throws IOException {
      // TODO Auto-generated method stub
      Map map=(HashMap)SerializationUtils.deserialize(bb);
    }

    @Override
    public void handleRecoverOk(String arg0) {
      // TODO Auto-generated method stub

    }

    @Override
    public void handleShutdownSignal(String arg0, ShutdownSignalException arg1) {
      // TODO Auto-generated method stub

    }
  }

  public abstract class EndPoint {
    protected Channel    channel;
    protected Connection connection;
    protected String     endPointName;

    public EndPoint(String endPointName) {
      try {
        this.endPointName = endPointName;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");
        connection = factory.newConnection();
        channel = connection.createChannel();
        /**
         * queue - the name of the queuedurable true - if we are declaring a
         * durable queue (the queue will survive a server restart)exclusive true
         * - if we are declaring an exclusive queue (restricted to this
         * connection)autoDelete true - if we are declaring an autodelete queue
         * (server will delete it when no longer in use)arguments other -
         * properties (construction arguments) for the queue
         */
        channel.queueDeclare(endPointName, true, false, false, null);
      } catch (Exception e) {
        System.out.println("出错了");

      }

    }

    public void close() throws IOException {
      this.channel.close();
      this.connection.close();
    }
  }

  
  private static void process(final String name, final BytesMessage req, final IStub cb) {

    new Task() {
      @Override
      public void onExecute() {

        try {
          long seq = req.readLong();
          String to = req.readUTF();
          String from = req.readUTF();

          long time = req.readLong();
          long delay = System.currentTimeMillis() - time;
          if (delay > 1000) {
            log.warn("MQ[" + name + "] reader delayed " + delay + "ms");
          }

          JSON message = null;
          int len = req.readInt();
          if (len > 0) {
            byte[] bb = new byte[len];
            req.readBytes(bb);
            ByteArrayInputStream is = new ByteArrayInputStream(bb);
            ObjectInputStream in = new ObjectInputStream(is);
            message = (JSON) in.readObject();
            in.close();
          }

          byte[] bb = null;
          len = req.readInt();
          if (len > 0) {
            bb = new byte[len];
            req.readBytes(bb);
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
   * broadcast the message as "topic" to all "dest:to", and return immediately
   * 
   * @param seq
   * @param to
   * @param message
   * @param bb
   * @param from
   * @return 1: success<br>
   */
  public static int broadcast(long seq, String to, JSON message, byte[] bb, String from) {
    if (message == null)
      return -1;

    if (!enabled) {
      return -1;
    }

    try {

      /**
       * get the message producer by destination name
       */
      MessageProducer p = getTopic(to);
      if (p != null) {
        BytesMessage resp = session.createBytesMessage();

        resp.writeLong(seq);
        resp.writeUTF(to);
        resp.writeUTF(from);
        resp.writeLong(System.currentTimeMillis());

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(os);
        out.writeObject(message);
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

        p.send(resp, DeliveryMode.NON_PERSISTENT, 0, X.AMINUTE);

        log.debug("Broadcasting:" + to + ", " + message);

        return 1;
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }

    return -1;
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

    if (!enabled) {
      return -1;
    }

    try {

      /**
       * get the message producer by destination name
       */
      MessageProducer p = getQueue(to);
      if (p != null) {
        BytesMessage resp = session.createBytesMessage();

        // Response resp = new Response();
        resp.writeLong(seq);
        resp.writeUTF(to == null ? X.EMPTY : to);
        resp.writeUTF(from == null ? X.EMPTY : from);
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

        p.send(resp, DeliveryMode.NON_PERSISTENT, 0, X.AMINUTE);

        channel.basicPublish("", endPointName, null, SerializationUtils.serialize(resp));
        
        log.debug("Sending:" + to + ", " + msg);

        return 1;
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }

    return -1;
  }

  /**
   * 获取消息队列的发送庄
   * 
   * @param name
   *          消息队列名称
   * @return messageproducer
   */
  private static Endpoint getQueue(String name) {
    synchronized (queues) {
      if (enabled) {
        if (queues.containsKey(name)) {
          return queues.get(name);
        }

        try {
          Destination dest = new ActiveMQQueue(group + name);
          MessageProducer producer = session.createProducer(dest);
          producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
          queues.put(name, producer);

          return producer;
        } catch (Exception e) {
          log.error(name, e);
        }
      }
    }

    return null;
  }

  private static MessageProducer getTopic(String name) {
    synchronized (topics) {
      if (enabled) {
        if (topics.containsKey(name)) {
          return topics.get(name);
        }

        try {
          Destination dest = new ActiveMQTopic(group + name);
          MessageProducer producer = session.createProducer(dest);
          producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
          topics.put(name, producer);

          return producer;
        } catch (Exception e) {
          log.error(name, e);
        }
      }
    }
    return null;
  }

  /**
   * queue producer cache
   */
  private static Map<String, MessageProducer> queues = new HashMap<String, MessageProducer>();

  /**
   * topic producer cache
   */
  private static Map<String, MessageProducer> topics = new HashMap<String, MessageProducer>();

  public static void main(String[] args) {
    Consumer consumer = new Consumer("river");  
    consumer.consumer();  
  }
  
}
