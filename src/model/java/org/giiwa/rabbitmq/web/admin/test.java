package org.giiwa.rabbitmq.web.admin;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.giiwa.core.json.JSON;
import org.giiwa.core.task.Task;
import org.giiwa.rabbitmq.mq.IStub;
import org.giiwa.rabbitmq.mq.MQ;

public class test {

  public static void main(String[] args) {

    PropertiesConfiguration pp = new PropertiesConfiguration();
    pp.addProperty("activemq.url",
        "failover:(tcp://joe.mac:61616)?timeout=3000&jms.prefetchPolicy.all=2&jms.useAsyncSend=true");

    Task.init(200, pp);

    MQ.init(pp);

    int n = 1000;
    int c = 100;
    Tester[] t = new Tester[c];
    for (int i = 0; i < t.length; i++) {
      t[i] = new Tester("t" + i, n);
      try {
        t[i].bind();
      } catch (Exception e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      t[i].send("echo", JSON.create(), null);
    }

    synchronized (t) {
      int i = 0;
      try {
        for (Tester t1 : t) {
          while (!t1.isFinished() && i < 100) {
            t.wait(1000);
            i++;
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    for (Tester t1 : t) {
      t1.println();
    }

    System.exit(0);

  }

  public static class Tester extends IStub {

    int        n;
    AtomicLong seq     = new AtomicLong();
    AtomicLong back    = new AtomicLong();
    AtomicLong total   = new AtomicLong();

    JSON       status  = JSON.create();
    JSON       msg;
    String     to;
    long       created = System.currentTimeMillis();

    public Tester(String name, int n) {
      super(name);
      this.n = n;
    }

    public void println() {
      System.out.println(status.toString());
    }

    public void send(String to, JSON msg, byte[] bb) {
      this.msg = msg;
      this.to = to;

      long s = seq.incrementAndGet();
      msg.put("sendtime", System.currentTimeMillis());
      this.send(s, to, msg, bb);
    }

    public boolean isFinished() {
      return back.get() == n;
    }

    @Override
    public void onRequest(long seq, String to, String from, JSON msg, byte[] attachment) {
      // System.out.println(msg);

      long min = status.getLong("min", Long.MAX_VALUE);
      long max = status.getLong("max", Long.MIN_VALUE);

      long t = System.currentTimeMillis() - msg.getLong("sendtime");
      total.addAndGet(t);
      if (t < min) {
        status.put("min", t);
      }
      if (t > max) {
        status.put("max", t);
      }
      status.put("total", total.get());

      back.incrementAndGet();
      status.put("aver", total.get() / back.get());

      if (this.seq.get() < n) {
        send(this.to, msg, null);
      }

      if (back.get() == n) {
        status.put("duration", System.currentTimeMillis() - created);
      }
    }

  }
}
