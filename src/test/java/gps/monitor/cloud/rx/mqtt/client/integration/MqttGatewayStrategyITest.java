package gps.monitor.cloud.rx.mqtt.client.integration;

import gps.monitor.cloud.rx.mqtt.client.conf.impl.MqttServerConfigApp;
import gps.monitor.cloud.rx.mqtt.client.conf.impl.PropertiesConfigApp;
import gps.monitor.cloud.rx.mqtt.client.enums.MessageBusStrategy;
import gps.monitor.cloud.rx.mqtt.client.subscriber.*;
import junit.framework.TestCase;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by daniel.carvajal on 20-06-2018.
 */
public class MqttGatewayStrategyITest extends TestCase {

    private MqttServerConfigApp mqttServerConfigApp;

    @Override
    protected void setUp() throws Exception {

    }

    @Test
    public void testSubcriberPublisherFastOkWithConnection10MessagesSecuenceStrategy() throws MqttException, InterruptedException {
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
                    $.subscriberBusStrategy = MessageBusStrategy.SECUENCE_STRATEGY;
                    $.messageSubscritor = new TestSubcriberConsumer(); //consumer
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
    public void testSubcriberSlowPublisherFastOkWithConnection10Messages() throws MqttException, InterruptedException {
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
                    $.subscriberBusStrategy = MessageBusStrategy.SECUENCE_STRATEGY;
                    $.messageSubscritor = new TestSlowSubcriberConsumer(); // slow consumer
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
        mqttGateway.detach(30000);
        mqttServerConfigApp.destroy();

        // check the messages
        TestSlowSubcriberConsumer messageSubcriptor = (TestSlowSubcriberConsumer) mqttGateway.getMessageSubcriptor();
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
    public void testSubcriberPublisherFastOkWithConnection20MessagesAsyncStrategy() throws MqttException, InterruptedException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();

        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        List messageSubcriptors = new ArrayList<>();
        messageSubcriptors.add(new TestSubcriberConsumer1());
        messageSubcriptors.add(new TestSubcriberConsumer2());
        //
        messageSubcriptors.add(new TestSlowSubcriberConsumer1());
        messageSubcriptors.add(new TestSlowSubcriberConsumer2());

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
                    $.subscriberBusStrategy = MessageBusStrategy.ASYNCHRONOUS_STRATEGY;
                    $.messageSubscritors = messageSubcriptors; // async consumers
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
        for(int i = 0; i < 20; i++){
            mqttGateway.publishWithOptions(String.format("%s.- Hello Word!!!", i));
        }
        // wait for send and receive message
        mqttGateway.detach(10000);
        mqttServerConfigApp.destroy();
    }

    @Test
    public void testSubcriberPublisherFastOkWithConnection20MessagesAsyncParallelStrategy() throws MqttException, InterruptedException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();

        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        List messageSubcriptors = new ArrayList<>();
        messageSubcriptors.add(new TestSubcriberConsumer1());
        messageSubcriptors.add(new TestSubcriberConsumer2());
        //
        messageSubcriptors.add(new TestSlowSubcriberConsumer1());
        messageSubcriptors.add(new TestSlowSubcriberConsumer2());

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
                    $.subscriberBusStrategy = MessageBusStrategy.ASYNCHRONOUS_PARALLEL_STRATEGY;
                    $.messageSubscritors = messageSubcriptors; // async paralell consumers
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
        for(int i = 0; i < 20; i++){
            mqttGateway.publishWithOptions(String.format("%s.- Hello Word!!!", i));
        }
        // wait for send and receive message
        mqttGateway.detach(10000);
        mqttServerConfigApp.destroy();
    }

    @Override
    protected void tearDown() throws Exception {

    }
}
