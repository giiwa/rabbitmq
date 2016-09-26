package org.giiwa.rabbitmq.mq;

import org.giiwa.core.json.JSON;
import org.giiwa.rabbitmq.mq.MQ.Mode;
import org.giiwa.rabbitmq.mq.MQ.Receiver;

/**
 * the message stub
 * 
 * @author joe
 *
 */
public abstract class IStub {

  private Receiver r;
  protected String name;

  public IStub(String name) {
    this.name = name;
  }

  final public void bind() {
    bind(Mode.QUEUE);
  }

  final public void bind(Mode m) {
    r = MQ.bind(name, this, m);
  }

  final public void close() {
    if (r != null) {
      r.close();
    }
  }

  public void send(long seq, String to, JSON msg, byte[] attachment) {
    MQ.send(seq, to, msg, attachment, name);
  }

  public abstract void onRequest(long seq, String to, String from, JSON msg, byte[] attachment);

}
