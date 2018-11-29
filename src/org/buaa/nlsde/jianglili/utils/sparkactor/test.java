package org.buaa.nlsde.jianglili.utils.sparkactor;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.handler.HandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;


import javax.servlet.*;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;

/**
 * Created by jianglili on 2017/2/27.
 */
public class test {

    public static void main(String[] args) throws Exception {

        Server server = new Server();
        Connector connector=new SelectChannelConnector();
        connector.setPort(Integer.getInteger("jetty.port",8080).intValue());
        connector.setHost("127.0.0.1");
        server.setConnectors(new Connector[]{connector});

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        server.setHandler(contexts);

        Context root = new Context(contexts,"/co",Context.SESSIONS);
        root.addServlet(new ServletHolder(new hello_one("Ciao")), "/*");
        Context yetanother =new Context(contexts,"/yo",Context.SESSIONS);
        yetanother.addServlet(new ServletHolder(new hello_two("YO!")), "/*");
        WebAppContext webapp = new WebAppContext(contexts,"webapp","/");
        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{root,yetanother,webapp});
        server.setHandler(handlers);
        server.start();
        server.join();

    }
  static class hello_two extends hello_one {
        private String name;
        public hello_two(String name){
            super(name);
            this.name=name;
        }

      @Override
      public void init(ServletConfig config) throws ServletException {

      }

      @Override
      public ServletConfig getServletConfig() {
          return null;
      }

      @Override
      public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {

      }

      @Override
      public String getServletInfo() {
          return null;
      }

      @Override
      public void destroy() {

      }
  }
    static class hello_one implements Servlet{
        private String name;
        public hello_one(String name){
            this.name=name;
        }

        @Override
        public void init(ServletConfig config) throws ServletException {

        }

        @Override
        public ServletConfig getServletConfig() {
            return null;
        }

        @Override
        public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {

        }

        @Override
        public String getServletInfo() {
            return null;
        }

        @Override
        public void destroy() {

        }
    }
}


abstract class TestMongodb4Jetty {

    public static void main(String[] args) throws Throwable {
        Server server = new Server(9090);
        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setWar("E:\\NutzQuickStart.war"); //经典的nutz入门例子
//        MongoSessionManager msm = new MongoSessionManager();
//        SessionHandler sessionHandler = new SessionHandler();
//        sessionHandler.setSessionManager(msm);
//        webAppContext.setSessionHandler(sessionHandler);
//        MongoSessionIdManager idMgr = new MongoSessionIdManager(server);
//        idMgr.setWorkerName("wendal-mongodb-worker");
//        idMgr.setScavengeDelay(60);
//        msm.setSessionIdManager(idMgr);
        server.setHandler(webAppContext);
        server.start();
    }
}

class Chat extends HttpServlet {
    public void doPost(HttpServletRequest request, HttpServletResponse response){
        postMessage(request, response);
    }
    private void postMessage(HttpServletRequest request, HttpServletResponse response)
    {
        HttpSession session = request.getSession(true);
//        People people = (People)session.getAttribute(session.getId());
//        while (!people.hasEvent())
//        {
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//        people.setContinuation(null);
//        people.sendEvent(response);
    }
}
