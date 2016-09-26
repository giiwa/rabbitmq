package org.giiwa.rabbitmq.mq;

import org.giiwa.core.json.JSON;

public class Echo extends IStub {

  public Echo(String name) {
    super(name);
  }

  @Override
  public void onRequest(long seq, String to, String from, JSON msg, byte[] bb) {
    msg.put("gottime", System.currentTimeMillis());
    MQ.send(seq, from, msg, bb, name);
  }

}
