package org.giiwa.rabbitmq.web;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.giiwa.core.bean.X;
import org.giiwa.core.conf.Global;
import org.giiwa.core.task.Task;
import org.giiwa.framework.bean.OpLog;
import org.giiwa.framework.web.IListener;
import org.giiwa.framework.web.Module;
import org.giiwa.rabbitmq.mq.Echo;
import org.giiwa.rabbitmq.mq.MQ;
import org.giiwa.rabbitmq.web.admin.rabbitmq;

public class RabbitmqListener implements IListener {

  static Log log = LogFactory.getLog(RabbitmqListener.class);

  @Override
  public void onStart(Configuration conf, Module m) {
    // TODO Auto-generated method stub
    log.info("rabbitmq is starting ...");

    // OpLog.info(activemq.class, "startup",
    // Global.getString("activemq.enabled", X.EMPTY), null, null);

    if (X.isSame("on", Global.getString("rabbitmq.enabled", X.EMPTY))) {
      new Task() {

        @Override
        public void onExecute() {
          MQ.init(conf);

          Echo e = new Echo("echo");
          try {
            e.bind();
          } catch (Exception e1) {
            log.error(e1.getMessage(), e1);
          }
        }

      }.schedule(10);
    } else {
      OpLog.info(rabbitmq.class, "startup", "disabled", null, null);
    }
  }

  @Override
  public void onStop() {
    // TODO Auto-generated method stub

  }

  @Override
  public void uninstall(Configuration conf, Module m) {
    // TODO Auto-generated method stub

  }

  @Override
  public void upgrade(Configuration conf, Module m) {
    // TODO Auto-generated method stub

  }

}
