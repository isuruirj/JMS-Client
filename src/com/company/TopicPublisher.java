package com.company;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

public class TopicPublisher {
    private String topicName = "news.sport";
    private String initialContextFactory = "org.wso2.andes.jndi."
            +"PropertiesFileInitialContextFactory";
    private String connectionString = "amqp:"
            +"//admin:admin@clientID/carbon?brokerlist='tcp://localhost:5675'";

    public static void main(String[] args) {
        TopicPublisher publisher = new TopicPublisher();
        publisher.publishWithTopicLookup();
    }

    public void publishWithTopicLookup() {
        Properties properties = new Properties();
        TopicConnection topicConnection = null;
        properties.put("java.naming.factory.initial", initialContextFactory);
        properties.put("connectionfactory.QueueConnectionFactory",
                connectionString);
        properties.put("topic." + topicName, topicName);

        try {
            // initialize
            // the required connection factories
            InitialContext ctx = new InitialContext(properties);
            TopicConnectionFactory topicConnectionFactory = (TopicConnectionFactory) ctx
                    .lookup("QueueConnectionFactory");
            topicConnection = topicConnectionFactory.createTopicConnection();

            try {
                TopicSession topicSession = topicConnection.createTopicSession(
                        false, Session.AUTO_ACKNOWLEDGE);
                // create or use the topic
                System.out.println("Use the Topic " + topicName);
                Topic topic = (Topic) ctx.lookup(topicName);
                javax.jms.TopicPublisher topicPublisher = topicSession
                        .createPublisher(topic);

                String msg = "Hi, I am Test Message";
                TextMessage textMessage = topicSession.createTextMessage(msg);

                topicPublisher.publish(textMessage);
                System.out.println("Publishing message " +textMessage);
                topicPublisher.close();
                topicSession.close();

                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } catch (JMSException e) {
            throw new RuntimeException("Error in JMS operations", e);
        } catch (NamingException e) {
            throw new RuntimeException("Error in initial context lookup", e);
        }
    }

}