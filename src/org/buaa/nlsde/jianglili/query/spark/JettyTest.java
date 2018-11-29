package org.buaa.nlsde.jianglili.query.spark;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.HttpStatus;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.json.JSONObject;
import scala.Console;

import static jdk.nashorn.internal.objects.Global.println;

public class JettyTest extends AbstractHandler{

    public static void main(String args[]) throws Exception
    {
        Server server = new Server(8082);
        server.setHandler(new JettyTest());
        server.start();
    }

    @Override
    public void handle(String s,
                       HttpServletRequest httpServletRequest,
                       HttpServletResponse httpServletResponse,
                       int i)
            throws IOException,
                   ServletException{
        //httpServletResponse.setContentType("text/html;charset=utf-8");
        //httpServletResponse.setStatus(HttpServletResponse.SC_OK);
        //((Request)httpServletRequest).setHandled(true);
        //httpServletResponse.getWriter().println("<h1>Hello World</h1>");


        //get the url and parameters
        String url = httpServletRequest.getRequestURI();
        httpServletResponse.setHeader("Access-Control-Allow-Origin", "*");
        httpServletResponse.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
        httpServletResponse.setHeader("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Origin");

        // get the query parameter
        String query = httpServletRequest.getParameter("q");
        String data = httpServletRequest.getParameter("data");
        if (query == null || data == null)
        {
            JSONObject jObject = new JSONObject();
            long start = System.currentTimeMillis();
            for(int ii = 0;ii<10000000;ii++)
            {

            }
            long timespend = System.currentTimeMillis() - start;
            try {
                jObject.put("time", timespend);

                System.out.println("time:" + timespend);

                jObject.put("result", "ok");
            }
            catch (JSONException e)
            {

            }
            httpServletResponse.setContentType("application/json;charset=utf-8");
            println(jObject);
            //httpServletResponse.getWriter().println(jObject);
            httpServletResponse.getWriter().println("<h1>Hello World</h1>");

            httpServletResponse.setStatus(HttpStatus.ORDINAL_200_OK);
            ((Request)httpServletRequest).setHandled(true);
            httpServletResponse.getWriter().close();

        }
        else
        {

        }
    }
}
