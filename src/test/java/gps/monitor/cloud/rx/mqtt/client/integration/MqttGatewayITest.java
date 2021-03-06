package gps.monitor.cloud.rx.mqtt.client.integration;

import gps.monitor.cloud.rx.mqtt.client.conf.impl.MqttServerConfigApp;
import gps.monitor.cloud.rx.mqtt.client.conf.impl.PropertiesConfigApp;
import gps.monitor.cloud.rx.mqtt.client.enums.MessageBusStrategy;
import gps.monitor.cloud.rx.mqtt.client.subscriber.TestSubcriberConsumer;
import junit.framework.TestCase;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Test;
import org.netcrusher.core.reactor.NioReactor;
import org.netcrusher.tcp.TcpCrusher;
import org.netcrusher.tcp.TcpCrusherBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created by daniel.carvajal on 20-06-2018.
 */
public class MqttGatewayITest extends TestCase {

    private MqttServerConfigApp mqttServerConfigApp;

    @Override
    protected void setUp() throws Exception {

    }
     /**
     * Construye un cliente basico y realiza una conexion al broker exitosa desde el cliente mqtt
     * con algunas opciones de conexion  y del buffer disponibles agregando un subcriptor a un topico mock
     *
     * @throws MqttException
     */
    @Test
    public void testSubscriberWithConnectionOk() throws MqttException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();
        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = true;
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = true;
                                $cOptions.keepAliveInterval = 60; //seg
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 30;
                                $cOptions.maxInflight = 1000;
                            }).createConnectOptions();
                })
                .with($ -> {
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = false;
                            }).createBufferOptions();
                })
                .with($ -> {
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with($sOptions -> {
                                $sOptions.topicFilter = "/topicMock";
                                $sOptions.qos = 2;
                            }).createSubcriberOptions();
                })
                .create()
         .subscribeWithOptions();
        //
        mqttServerConfigApp.destroy();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa desde el cliente mqtt
     * con algunas opciones de conexion  y del buffer disponibles agregando un subcriptor a un topico mock y
     * un publicador al mismo topico enviando un mensaje
     *
     * @throws MqttException
     */
    @Test
    public void testPublisherOkWithConnection() throws MqttException, InterruptedException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();
        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = false;
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = true;
                                $cOptions.keepAliveInterval = 60; //seg
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 30;
                                $cOptions.maxInflight = 1000;
                            }).createConnectOptions();
                })
                .with($ -> {
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = false;
                            }).createBufferOptions();
                })
                .with($ -> {
                    $.publisherOptions = new MqttGatewayBuilder.PublisherOptionsBuilder()
                            .with($pOptions -> {
                                $pOptions.topicFilter = "/topicMock";
                                $pOptions.qos = 2;
                                $pOptions.retained = false;
                            }).createPublisherOptions();
                })
         .create();
        //
        mqttGateway.publishWithOptions("Hello Word!!!");

        // wait for send and receive message
        mqttGateway.detach(500);
        mqttServerConfigApp.destroy();
    }


    @Test
    public void testSubcriberPublisherOkWithConnection1Message() throws MqttException, InterruptedException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();
        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = false;
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = true;
                                $cOptions.keepAliveInterval = 60; //seg
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 30;
                                $cOptions.maxInflight = 1000;
                            }).createConnectOptions();
                })
                .with($ -> {
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = false;
                            }).createBufferOptions();
                })
                .with($ -> {
                    //$.subscriberBusStrategy = MessageBusStrategy.SECUENCE_STRATEGY; // default strategy
                    $.messageSubscritor = new TestSubcriberConsumer();
                })
                .with($ -> {
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with($sOptions -> {
                                $sOptions.topicFilter = "/topicMock";
                                $sOptions.qos = 0;
                            }).createSubcriberOptions();
                })
                .with($ -> {
                    $.publisherOptions = new MqttGatewayBuilder.PublisherOptionsBuilder()
                            .with($pOptions -> {
                                $pOptions.topicFilter = "/topicMock";
                                $pOptions.qos = 0;
                                $pOptions.retained = false;
                            }).createPublisherOptions();
                })
                .create()
        .subscribeWithOptions();
        //
        mqttGateway.publishWithOptions("Hello Word!!!");

        // wait for send and receive message
        mqttGateway.detach(5000);
        mqttServerConfigApp.destroy();

        // check the message
        TestSubcriberConsumer messageSubcriptor = (TestSubcriberConsumer) mqttGateway.getMessageSubcriptor();
        assertTrue(messageSubcriptor.getMessagesReceived().size() == 1);
        //
        String payload = new String(messageSubcriptor.getMessagesReceived().get(0).getPayload(), StandardCharsets.UTF_8);
        String topic   = messageSubcriptor.getMessagesReceived().get(0).getTopicFilter();
        int qos        = messageSubcriptor.getMessagesReceived().get(0).getQos();
        //
        assertEquals(payload, "Hello Word!!!");
        assertEquals(topic, "/topicMock");
        assertEquals(qos, 0);

    }

    @Test
    public void testSubcriberAsyncPublisherOkWithConnection4Messages() throws MqttException, InterruptedException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();
        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();
        //
        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    // config desa
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = false;
                    $.memoryPersistence = false;
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = true;
                                $cOptions.keepAliveInterval = 60; //seg
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 30;
                                $cOptions.maxInflight = 1000;
                            }).createConnectOptions();
                })
                .with($ -> {
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = false;
                            }).createBufferOptions();
                })
                .with($ -> {
                    $.subscriberBusStrategy = MessageBusStrategy.SECUENCE_STRATEGY;
                    $.messageSubscritor = new TestSubcriberConsumer();
                })
                .with($ -> {
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with($sOptions -> {
                                $sOptions.topicFilter = "/topicMock";
                                $sOptions.qos = 0;
                            }).createSubcriberOptions();
                })
                .with($ -> {
                    $.publisherOptions = new MqttGatewayBuilder.PublisherOptionsBuilder()
                            .with($pOptions -> {
                                $pOptions.topicFilter = "/topicMock";
                                $pOptions.qos = 0;
                                $pOptions.retained = false;
                            }).createPublisherOptions();
                })
                .create()
        .subscribeWithOptions();
        //
        mqttGateway.publishWithOptions("1.- Hello Word!!!");
        mqttGateway.publishWithOptions("2.- Hello Word!!!");
        mqttGateway.publishWithOptions("3.- Hello Word!!!");
        mqttGateway.publishWithOptions("4.- Hello Word!!!");

        // wait for send and receive message
        mqttGateway.detach(5000);
        mqttServerConfigApp.destroy();

        // check the messages
        TestSubcriberConsumer messageSubcriptor = (TestSubcriberConsumer) mqttGateway.getMessageSubcriptor();
        assertTrue(messageSubcriptor.getMessagesReceived().size() == 4);
        // 1 message
        String payload1 = new String(messageSubcriptor.getMessagesReceived().get(0).getPayload(), StandardCharsets.UTF_8);
        String topic1   = messageSubcriptor.getMessagesReceived().get(0).getTopicFilter();
        int qos1        = messageSubcriptor.getMessagesReceived().get(0).getQos();
        //
        assertEquals(payload1, "1.- Hello Word!!!");
        assertEquals(topic1, "/topicMock");
        assertEquals(qos1, 0);

        // 2 message
        String payload2 = new String(messageSubcriptor.getMessagesReceived().get(1).getPayload(), StandardCharsets.UTF_8);
        String topic2   = messageSubcriptor.getMessagesReceived().get(1).getTopicFilter();
        int qos2        = messageSubcriptor.getMessagesReceived().get(1).getQos();
        //
        assertEquals(payload2, "2.- Hello Word!!!");
        assertEquals(topic2, "/topicMock");
        assertEquals(qos2, 0);

        // 3 message
        String payload3 = new String(messageSubcriptor.getMessagesReceived().get(2).getPayload(), StandardCharsets.UTF_8);
        String topic3   = messageSubcriptor.getMessagesReceived().get(2).getTopicFilter();
        int qos3        = messageSubcriptor.getMessagesReceived().get(2).getQos();
        //
        assertEquals(payload3, "3.- Hello Word!!!");
        assertEquals(topic3, "/topicMock");
        assertEquals(qos3, 0);

        // 4 message
        String payload4 = new String(messageSubcriptor.getMessagesReceived().get(3).getPayload(), StandardCharsets.UTF_8);
        String topic4   = messageSubcriptor.getMessagesReceived().get(3).getTopicFilter();
        int qos4        = messageSubcriptor.getMessagesReceived().get(3).getQos();
        //
        assertEquals(payload4, "4.- Hello Word!!!");
        assertEquals(topic4, "/topicMock");
        assertEquals(qos4, 0);
    }

    @Test
    public void testSubcriberPublisherOkWithConnection10Messages() throws MqttException, InterruptedException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();

        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    // config desa
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = false;
                    $.memoryPersistence = false;
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = true;
                                $cOptions.keepAliveInterval = 60; //seg
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 30;
                                $cOptions.maxInflight = 1000;
                            }).createConnectOptions();
                })
                .with($ -> {
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = false;
                            }).createBufferOptions();
                })
                .with($ -> {
                    $.messageSubscritor = new TestSubcriberConsumer();
                    $.subscriberBusStrategy = MessageBusStrategy.SECUENCE_STRATEGY;
                    //
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with($sOptions -> {
                                $sOptions.topicFilter = "/topicMock";
                                $sOptions.qos = 0;
                            }).createSubcriberOptions();
                })
                .with($ -> {
                    $.publisherOptions = new MqttGatewayBuilder.PublisherOptionsBuilder()
                            .with($pOptions -> {
                                $pOptions.topicFilter = "/topicMock";
                                $pOptions.qos = 0;
                                $pOptions.retained = false;
                            }).createPublisherOptions();
                })
                .create()
        .subscribeWithOptions();
        //
        for(int i = 0; i < 10; i++){
            mqttGateway.publishWithOptions(String.format("%s.- Hello Word!!!", i));
        }
        // wait for send and receive message
        mqttGateway.detach(10000);
        mqttServerConfigApp.destroy();

        // check the messages
        TestSubcriberConsumer messageSubcriptor = (TestSubcriberConsumer) mqttGateway.getMessageSubcriptor();
        assertTrue(messageSubcriptor.getMessagesReceived().size() == 10);

        // 10 message
        for(int i = 0; i < 10; i++) {
            String payload = new String(messageSubcriptor.getMessagesReceived().get(i).getPayload(), StandardCharsets.UTF_8);
            String topic = messageSubcriptor.getMessagesReceived().get(i).getTopicFilter();
            int qos = messageSubcriptor.getMessagesReceived().get(i).getQos();
            //
            assertEquals(payload, String.format("%s.- Hello Word!!!", i));
            assertEquals(topic, "/topicMock");
            assertEquals(qos, 0);
        }
    }

    @Test
    public void testSubcriberPublisherOkWithOutConnectionAndPersistBuffer20MessagesNoSend() throws MqttException, InterruptedException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();

        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        // config mqtt gateway
        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    // config desa
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = false;
                    $.memoryPersistence = false;
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = true;
                                $cOptions.keepAliveInterval = 60; //seg
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 30;
                                $cOptions.maxInflight = 1000;
                            }).createConnectOptions();
                })
                .with($ -> {
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = true;
                                $bOptions.bufferSize = 1000;
                                $bOptions.persistBuffer = true;
                                $bOptions.deleteOldestMessages = false;
                            }).createBufferOptions();
                })
                .with($ -> {
                    $.messageSubscritor = new TestSubcriberConsumer();
                    $.subscriberBusStrategy = MessageBusStrategy.SECUENCE_STRATEGY;
                    //
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with($sOptions -> {
                                $sOptions.topicFilter = "/topicMock";
                                $sOptions.qos = 0;
                            }).createSubcriberOptions();
                })
                .with($ -> {
                    $.publisherOptions = new MqttGatewayBuilder.PublisherOptionsBuilder()
                            .with($pOptions -> {
                                $pOptions.topicFilter = "/topicMock";
                                $pOptions.qos = 0;
                                $pOptions.retained = false;
                            }).createPublisherOptions();
                })
                .create()
        .subscribeWithOptions();

        // send 20 messages
        for(int i = 0; i < 20; i++){
            mqttGateway.publishWithOptions(String.format("%s.- Hello Word!!!", i));
            // wait for send and receive message
            Thread.sleep(1000);

            if(i == 9) { // 10 elements disconect client
                mqttGateway.detach();
            }
        }

        // check the messages
        TestSubcriberConsumer messageSubcriptor = (TestSubcriberConsumer) mqttGateway.getMessageSubcriptor();
        assertTrue(messageSubcriptor.getMessagesReceived().size() == 10); // only 10 accept

        // ckeck 10 message in the message subscriber
        for(int i = 0; i < 10; i++) {
            String payload = new String(messageSubcriptor.getMessagesReceived().get(i).getPayload(), StandardCharsets.UTF_8);
            String topic = messageSubcriptor.getMessagesReceived().get(i).getTopicFilter();
            int qos = messageSubcriptor.getMessagesReceived().get(i).getQos();
            //
            assertEquals(payload, String.format("%s.- Hello Word!!!", i));
            assertEquals(topic, "/topicMock");
            assertEquals(qos, 0);
        }

        // check how many messages have the buffer(10 messages)
        int messageBufferCount = mqttGateway.getMqttAsyncClient().getBufferedMessageCount();
        assertEquals(messageBufferCount, 10);
        for(int j = 0, k = 10; j < messageBufferCount; j++){
            MqttMessage message = mqttGateway.getMqttAsyncClient().getBufferedMessage(j);
            assertEquals(new String(message.getPayload(), StandardCharsets.UTF_8), String.format("%s.- Hello Word!!!", (j + k)));
            assertEquals(message.getQos(), 0);
        }

        //mqttGateway.detach();
        mqttServerConfigApp.destroy();
    }

    @Test
    public void testSubcriberPublisherOkWithOutConnectionAndPersistBuffer20MessagesSend() throws MqttException, InterruptedException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();

        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        // config mqtt gateway
        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    // config desa
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = false;
                    $.memoryPersistence = false;
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = true;
                                $cOptions.keepAliveInterval = 60; //seg
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 30;
                                $cOptions.maxInflight = 1000;
                            }).createConnectOptions();
                })
                .with($ -> {
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = true;
                                $bOptions.bufferSize = 1000;
                                $bOptions.persistBuffer = true;
                            }).createBufferOptions();
                })
                .with($ -> {
                    $.messageSubscritor = new TestSubcriberConsumer();
                    $.subscriberBusStrategy = MessageBusStrategy.SECUENCE_STRATEGY;
                    //
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with($sOptions -> {
                                $sOptions.topicFilter = "/topicMock";
                                $sOptions.qos = 0;
                            }).createSubcriberOptions();
                })
                .with($ -> {
                    $.publisherOptions = new MqttGatewayBuilder.PublisherOptionsBuilder()
                            .with($pOptions -> {
                                $pOptions.topicFilter = "/topicMock";
                                $pOptions.qos = 0;
                                $pOptions.retained = false;
                            }).createPublisherOptions();
                })
                .create()
         .subscribeWithOptions();

        // send 20 messages
        for(int i = 0; i < 20; i++){
            mqttGateway.publishWithOptions(String.format("%s.- Hello Word!!!", i));
            // wait for send and receive message
            Thread.sleep(1000);

            if(i == 9) { // 10 elements disconect client
                mqttGateway.getMqttAsyncClient().disconnect(); // disconect
            }
        }

        // check the messages
        TestSubcriberConsumer messageSubcriptor = (TestSubcriberConsumer) mqttGateway.getMessageSubcriptor();
        assertTrue(messageSubcriptor.getMessagesReceived().size() == 10); // only 10 accept

        // ckeck 10 message in the message subscriber
        for(int i = 0; i < 10; i++) {
            String payload = new String(messageSubcriptor.getMessagesReceived().get(i).getPayload(), StandardCharsets.UTF_8);
            String topic = messageSubcriptor.getMessagesReceived().get(i).getTopicFilter();
            int qos = messageSubcriptor.getMessagesReceived().get(i).getQos();
            //
            assertEquals(payload, String.format("%s.- Hello Word!!!", i));
            assertEquals(topic, "/topicMock");
            assertEquals(qos, 0);
        }

        // check how many messages have the buffer(10 messages)
        int messageBufferCount = mqttGateway.getMqttAsyncClient().getBufferedMessageCount();
        assertEquals(messageBufferCount, 10);
        for(int j = 0, k = 10; j < messageBufferCount; j++){
            MqttMessage message = mqttGateway.getMqttAsyncClient().getBufferedMessage(j);
            assertEquals(new String(message.getPayload(), StandardCharsets.UTF_8), String.format("%s.- Hello Word!!!", (j + k)));
            assertEquals(message.getQos(), 0);
        }

        // wait for reconect
        mqttGateway.getMqttAsyncClient().reconnect();
        while(!mqttGateway.isConnected()){
        }
        // wait for send buffer messages
        Thread.sleep(1000);

        // check how many messages have the buffer(0 messages) after the connection
        messageBufferCount = mqttGateway.getMqttAsyncClient().getBufferedMessageCount();
        assertEquals(messageBufferCount, 0);

        mqttGateway.detach();
        mqttServerConfigApp.destroy();
    }

    @Test
    public void testRetryConnectionOnlyCreateClient() throws MqttException, InterruptedException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();

        // config mqtt gateway
        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    // config desa
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = false;
                    $.memoryPersistence = false;
                    $.retryAndWait = true; // save attach
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = true;
                                $cOptions.keepAliveInterval = 60; //seg
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 30;
                                $cOptions.maxInflight = 1000;
                            }).createConnectOptions();
                })
                .with($ -> {
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = true;
                                $bOptions.bufferSize = 1000;
                                $bOptions.persistBuffer = true;
                            }).createBufferOptions();
                })
         .create();

         assertTrue(!mqttGateway.isConnected());
         // wait for evaluate connection in backgroud
         Thread.sleep(20000);

        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        // wait to server respond
        Thread.sleep(5000);
        assertTrue(mqttGateway.isConnected());

        mqttGateway.detach();
        mqttServerConfigApp.destroy();
    }

    @Test
    public void testRetryConnectionCreateClientWithSubcription() throws MqttException, InterruptedException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();

        // config mqtt gateway
        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    // config desa
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = false;
                    $.retryAndWait = true; // save attach
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = true;
                                $cOptions.keepAliveInterval = 60; //seg
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 30;
                                $cOptions.maxInflight = 1000;
                            }).createConnectOptions();
                })
                .with($ -> {
                    $.memoryPersistence = false;
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = true;
                                $bOptions.bufferSize = 1000;
                                $bOptions.persistBuffer = true;
                            }).createBufferOptions();
                })
                .with($ -> {
                    $.messageSubscritor = new TestSubcriberConsumer();
                    $.subscriberBusStrategy = MessageBusStrategy.SECUENCE_STRATEGY;
                    //
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with($sOptions -> {
                                $sOptions.topicFilter = "/topicMock";
                                $sOptions.qos = 0;
                            }).createSubcriberOptions();
                })
        .create();

        assertTrue(!mqttGateway.isConnected());
        // wait for evaluate connection in backgroud
        Thread.sleep(20000);

        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        // wait to server respond
        Thread.sleep(1000);
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);

        mqttGateway.detach();
        mqttServerConfigApp.destroy();
    }

    @Test
    public void testRetryConnectionOnlyCreateClientSubcriptionAndPublishMessage() throws MqttException, InterruptedException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();

        // config mqtt gateway
        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    // config desa
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = false;
                    $.retryAndWait = true; // save attach
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = true;
                                $cOptions.keepAliveInterval = 60; //seg
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 30;
                                $cOptions.maxInflight = 1000;
                    }).createConnectOptions();
                })
                .with($ -> {
                    $.memoryPersistence = false;
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = true;
                                $bOptions.bufferSize = 1000;
                                $bOptions.persistBuffer = true;
                    }).createBufferOptions();
                })
                .with($ -> {
                    $.messageSubscritor = new TestSubcriberConsumer();
                    $.subscriberBusStrategy = MessageBusStrategy.SECUENCE_STRATEGY;
                    //
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with($sOptions -> {
                                $sOptions.topicFilter = "/topicMock";
                                $sOptions.qos = 0;
                    }).createSubcriberOptions();
                })
                .with($ -> {
                    $.publisherOptions = new MqttGatewayBuilder.PublisherOptionsBuilder()
                            .with($pOptions -> {
                                $pOptions.topicFilter = "/topicMock";
                                $pOptions.qos = 0;
                                $pOptions.retained = false;
                    }).createPublisherOptions();
                })
         .create();

        assertTrue(!mqttGateway.isConnected());
        // wait for evaluate connection in background
        Thread.sleep(20000);

        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        // wait to server respond
        Thread.sleep(1000);
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);

        // publish message
        mqttGateway.publishWithOptions("Hello Word!!!");

        // wait for send and receive message
        mqttGateway.detach(5000);
        mqttServerConfigApp.destroy();

        // check the message
        TestSubcriberConsumer messageSubcriptor = (TestSubcriberConsumer) mqttGateway.getMessageSubcriptor();
        assertTrue(messageSubcriptor.getMessagesReceived().size() == 1);
        //
        String payload = new String(messageSubcriptor.getMessagesReceived().get(0).getPayload(), StandardCharsets.UTF_8);
        String topic   = messageSubcriptor.getMessagesReceived().get(0).getTopicFilter();
        int qos        = messageSubcriptor.getMessagesReceived().get(0).getQos();
        //
        assertEquals(payload, "Hello Word!!!");
        assertEquals(topic, "/topicMock");
        assertEquals(qos, 0);
    }

    @Override
    protected void tearDown() throws Exception {

    }
}
