package com.company;

import com.sun.corba.se.impl.oa.toa.TOA;
import org.apache.log4j.Logger;

import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class TopicSubscriber {

    final static Logger logger = Logger.getLogger(TopicSubscriber.class);

    private String topicName = "news.sport";
    private String initialContextFactory = "org.wso2.andes.jndi."
            +"PropertiesFileInitialContextFactory";
    private String connectionString = "amqp:"
            +"//admin:admin@clientID/carbon?brokerlist='tcp://localhost:5675'";
    private boolean messageReceived = false;

    public static void main(String[] args) {
        TopicSubscriber subscriber = new TopicSubscriber();
        subscriber.subscribeWithTopicLookup();
    }

    public void subscribeWithTopicLookup() {

        Properties properties = new Properties();
        TopicConnection topicConnection = null;
        properties.put("java.naming.factory.initial", initialContextFactory);
        properties.put("connectionfactory.QueueConnectionFactory",
                connectionString);
        properties.put("topic." + topicName, topicName);
        try {
            InitialContext ctx = new InitialContext(properties);
            TopicConnectionFactory topicConnectionFactory = (TopicConnectionFactory) ctx
                    .lookup("QueueConnectionFactory");
            topicConnection = topicConnectionFactory.createTopicConnection();
            System.out
                    .println("Create Topic Connection for Topic " + topicName);

            while (!messageReceived) {
                try {
                    TopicSession topicSession = topicConnection
                            .createTopicSession(false, Session.CLIENT_ACKNOWLEDGE);

                  //  topicSession.recover();

                    Topic topic = (Topic) ctx.lookup(topicName);
                    // start the connection
                    topicConnection.start();

                    // create a topic subscriber
                    javax.jms.TopicSubscriber topicSubscriber = topicSession
                            .createSubscriber(topic);

                  //  TestMessageListener messageListener = new TestMessageListener();
                  //  topicSubscriber.setMessageListener(messageListener);

                    int x=1000;
                    while(x>0){

                        for(int i=0; i<5; i++){

                            logger.info("message "+i+": "+topicSubscriber.receive());
                        }
                        topicSession.recover();

                        x--;
                    }
                   // topicSubscriber.receive();

                    Thread.sleep(5000);
                    topicSubscriber.close();
                    topicSession.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                } catch (NamingException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        } catch (NamingException e) {
            throw new RuntimeException("Error in initial context lookup", e);
        } catch (JMSException e) {
            throw new RuntimeException("Error in JMS operations", e);
        } finally {
            if (topicConnection != null) {
                try {
                    topicConnection.close();
                } catch (JMSException e) {
                    throw new RuntimeException(
                            "Error in closing topic connection", e);
                }
            }
        }
    }

    public class TestMessageListener implements MessageListener {
        public void onMessage(Message message) {
            try {
                System.out.println("Got the Message : "
                        + ((TextMessage) message).getText());
                messageReceived = true;
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

}