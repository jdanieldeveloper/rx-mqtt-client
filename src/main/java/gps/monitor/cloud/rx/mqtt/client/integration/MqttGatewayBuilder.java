package gps.monitor.cloud.rx.mqtt.client.integration;

import gps.monitor.cloud.rx.mqtt.client.bus.Bus;
import gps.monitor.cloud.rx.mqtt.client.bus.impl.MessagePublisherBus;
import gps.monitor.cloud.rx.mqtt.client.bus.impl.MessageSubscriberAsyncBus;
import gps.monitor.cloud.rx.mqtt.client.bus.impl.MessageSubscriberAsyncParallelBus;
import gps.monitor.cloud.rx.mqtt.client.bus.impl.MessageSubscriberBus;
import gps.monitor.cloud.rx.mqtt.client.enums.MessageBusStrategy;
import gps.monitor.cloud.rx.mqtt.client.subscriber.MessageConsumer;
import gps.monitor.cloud.rx.mqtt.client.publisher.MessagePublicator;
import gps.monitor.cloud.rx.mqtt.client.publisher.MessagePublisher;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.eclipse.paho.client.mqttv3.util.Debug;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Functional Builder Pattern para construir un {@link MqttGateway}
 *
 * @author daniel.carvajal
 */
public class MqttGatewayBuilder {

    public String host;
    public int port;
    public String identifier;

    public boolean memoryPersistence;

    public boolean debug;

    public boolean retryAndWait;

    // connect options
    public MqttConnectOptions connectOptions;

    // buffer options
    public DisconnectedBufferOptions bufferOptions;

    // subcriber
    public MessageConsumer messageSubscritor;
    public List<Consumer<Object>> messageSubscritors;
    public MessageBusStrategy subscriberBusStrategy;
    // subscriber options
    public MqttSubscriberOptions subscriberOptions;

    // publisher
    public MessagePublisher messagePublicator;
    public MessageBusStrategy publisherBusStrategy;
    // publisher options
    public MqttPublisherOptions publisherOptions;

    // gateway
    private MqttGateway mqttGateway;

    private static MqttGatewayBuilder mqttGatewayBuilder;

    private static final String PROTOCOL = "tcp";

    private static final Logger logger = LoggerFactory.getLogger(MqttGatewayBuilder.class);

    /**
     * Singleton para el builder
     *
     * @return nueva instancia del builder
     */
    public static MqttGatewayBuilder getInstance() {
        if (mqttGatewayBuilder == null) {
            mqttGatewayBuilder = new MqttGatewayBuilder();
        }
        return mqttGatewayBuilder;
    }

    /**
     * Consolida todos los parametros del {@link MqttGatewayBuilder}
     *
     * @param builder {@link Consumer} para setear los parametros
     * @return la instancia del {@link MqttGatewayBuilder}
     */
    public MqttGatewayBuilder with(Consumer<MqttGatewayBuilder> builder) {
        builder.accept(this);
        return this;
    }

    /**
     * Crea un {@link MqttGateway} con todos sus parametros asociados a traves del {@link MqttGatewayBuilder}
     *
     * @return una nueva instancia del {@link MqttGateway}
     */
    public MqttGateway create() {
        mqttGateway = new MqttGateway();
        String serverUri = String.format("%s://%s:%s", PROTOCOL, host, port);
        //
        mqttGateway.setProtocol(PROTOCOL);
        mqttGateway.setHost(host);
        mqttGateway.setPort(port);
        mqttGateway.setIdentifier(identifier);
        //
        MqttAsyncClient mqttAsyncClient;
        try {
            // persistence type
            if(!memoryPersistence) {
                mqttAsyncClient = new MqttAsyncClient(serverUri, identifier, new MqttDefaultFilePersistence());
            }else{
                mqttAsyncClient = new MqttAsyncClient(serverUri, identifier, new MemoryPersistence());
            }
            mqttGateway.setMemoryPersistence(memoryPersistence);

            // create mqtt client and attach
            createClientOptions();

            mqttGateway.setMqttAsyncClient(mqttAsyncClient);
            if(!retryAndWait) {
                mqttGateway.attach();
            }else{
                RetryConnection retryConnection = RetryConnection.getInstance(mqttGateway);
                retryConnection.start();
            }
            // message publisher
            createMessagePublisher();

            // message subscriber
            createMessageSubscriber();

            if(debug){
                Debug cDebug = mqttAsyncClient.getDebug();
                cDebug.dumpClientDebug();
            }

        } catch (MqttException e) {
            logger.error("[{}] Error en la creacion del cliente Mqtt!!!", MqttAsyncClient.class.getSimpleName());
            logger.error(e.getMessage(), e);

        } catch (Exception e) {
            logger.error("[}] Error en la creacion del cliente Mqtt!!!", MqttAsyncClient.class.getSimpleName());
            logger.error(e.getMessage(), e);
        }
        return mqttGateway;
    }

    /**
     * Crea las siguientes opciones para el {@link MqttGateway}
     *
     * - {@link DisconnectedBufferOptions}
     * - {@link MqttConnectOptions}
     * - {@link MqttSubscriberOptions}
     * - {@link MqttPublisherOptions}
     *
     */
    public void createClientOptions() {
        if(Objects.nonNull(bufferOptions)){
            mqttGateway.setBufferOptions(bufferOptions);
        }else{
            // default options
            mqttGateway.setBufferOptions(new DisconnectedBufferOptions());
        }
        if(Objects.nonNull(connectOptions)){
            mqttGateway.setConnectOptions(connectOptions);
        }else{
            // default options
            mqttGateway.setConnectOptions(new MqttConnectOptions());
        }
        if(Objects.nonNull(subscriberOptions)){
            mqttGateway.setSubscriberOptions(subscriberOptions);
        }else{
            // default options
            mqttGateway.setSubscriberOptions(new MqttSubscriberOptions());
        }
        if(Objects.nonNull(publisherOptions)){
            mqttGateway.setPublisherOptions(publisherOptions);
        } else{
            // default options
            mqttGateway.setPublisherOptions(new MqttPublisherOptions());
        }
    }

    /**
     * Crea el {@link Bus} de subcripcion con todos sus parametros
     *
     */
    public void createMessageSubscriber(){
        Bus messageSubscriberBus = null;
        if(Objects.nonNull(subscriberBusStrategy)) {
            switch (subscriberBusStrategy) {
                case ASYNCHRONOUS_STRATEGY:
                    messageSubscriberBus = MessageSubscriberAsyncBus.getInstance();
                    messageSubscriberBus.subscribe(messageSubscritors);
                    mqttGateway.setMessageSubscriberBus(messageSubscriberBus);
                    mqttGateway.setMessageSubscritors(messageSubscritors);
                    break;
                case ASYNCHRONOUS_PARALLEL_STRATEGY:
                    messageSubscriberBus = MessageSubscriberAsyncParallelBus.getInstance();
                    messageSubscriberBus.subscribe(messageSubscritor);
                    mqttGateway.setMessageSubscriberBus(messageSubscriberBus);
                    mqttGateway.setMessageSubscritors(messageSubscritors);
                    break;
                default:
                    // default strategy SECUENCE_STRATEGY
                    messageSubscriberBus = MessageSubscriberBus.getInstance();
                    messageSubscriberBus.subscribe(messageSubscritor);
                    mqttGateway.setMessageSubscriberBus(messageSubscriberBus);
                    mqttGateway.setMessageSubcriptor(messageSubscritor);
                    break;
            }
        }else{
            // default strategy
            messageSubscriberBus = MessageSubscriberBus.getInstance();
            messageSubscriberBus.subscribe(messageSubscritor);
            mqttGateway.setMessageSubscriberBus(messageSubscriberBus);
            mqttGateway.setMessageSubcriptor(messageSubscritor);
        }
    }

    /**
     * Crea el {@link Bus} de publicacion con todos sus parametros
     *
     */
    public void createMessagePublisher(){
        Bus messagePublisherBus = null;
        if(Objects.nonNull(publisherBusStrategy)) {
            switch (publisherBusStrategy) {
                case SECUENCE_STRATEGY:
                    messagePublicator = new MessagePublicator();
                    messagePublicator.setMqttGateway(mqttGateway);
                    //
                    messagePublisherBus = MessagePublisherBus.getInstance();
                    messagePublisherBus.subscribe(messagePublicator);
                    break;
                default:
                    // default strategy
                    messagePublicator = new MessagePublicator();
                    messagePublicator.setMqttGateway(mqttGateway);
                    //
                    messagePublisherBus = MessagePublisherBus.getInstance();
                    messagePublisherBus.subscribe(messagePublicator);
                    break;
            }
        }else{
            // default strategy
            messagePublicator = new MessagePublicator();
            messagePublicator.setMqttGateway(mqttGateway);
            //
            messagePublisherBus = MessagePublisherBus.getInstance();
            messagePublisherBus.subscribe(messagePublicator);
        }
        mqttGateway.setMessagePublicator(messagePublicator);
        mqttGateway.setMessagePublisherBus(messagePublisherBus);

    }

    /**
     * Functional Builder Pattern para construir un {@link MqttConnectOptions}
     *
     * @author daniel.carvajal
     */
    public static class ConnectOptionsBuilder {
        public boolean cleanSession;
        public int keepAliveInterval;
        public boolean automaticReconnect;
        public int connectionTimeout;
        public int maxInflight;

        /**
         * Constructor con parametros por defecto. Ver los parametros por defecto de la clase {@link MqttConnectOptions}
         */
        public ConnectOptionsBuilder(){
            // default values
            this.cleanSession = true;
            this.keepAliveInterval = 60; //seg
            this.automaticReconnect = false;
            this.connectionTimeout = 30;
            this.maxInflight = 10;
        }

        /**
         * Consolida todos los parametros del {@link ConnectOptionsBuilder}
         *
         * @param builder {@link Consumer} para setear los parametros
         * @return la instancia del {@link ConnectOptionsBuilder}
         */
        public ConnectOptionsBuilder with(Consumer<ConnectOptionsBuilder> builder) {
            builder.accept(this);
            return this;
        }

        /**
         * Crea las opciones de conexion para el {@link MqttGateway}
         *
         * @return las opciones de conexion
         */
        public MqttConnectOptions createConnectOptions() {
            MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
            mqttConnectOptions.setCleanSession(cleanSession);
            mqttConnectOptions.setKeepAliveInterval(keepAliveInterval);
            mqttConnectOptions.setAutomaticReconnect(automaticReconnect);
            mqttConnectOptions.setConnectionTimeout(connectionTimeout);
            mqttConnectOptions.setMaxInflight(maxInflight);
            return mqttConnectOptions;
        }
    }


    /**
     * Functional Builder Pattern para construir un {@link BufferOptionsBuilder}
     *
     * @author daniel.carvajal
     */
    public static class BufferOptionsBuilder {
        public boolean bufferEnabled;
        public boolean persistBuffer;
        public int bufferSize;
        public boolean deleteOldestMessages;

        /**
         * Constructor con parametros por defecto. Ver los parametros por defecto de la clase {@link DisconnectedBufferOptions}
         */
        public BufferOptionsBuilder(){
            // default values
            this.bufferEnabled = false;
            this.persistBuffer = false;
            this.bufferSize = 5000;
            this.deleteOldestMessages = false;
        }

        /**
         * Consolida todos los parametros del {@link BufferOptionsBuilder}
         *
         * @param builder {@link Consumer} para setear los parametros
         * @return la instancia del {@link BufferOptionsBuilder}
         */
        public BufferOptionsBuilder with(Consumer<BufferOptionsBuilder> builder) {
            builder.accept(this);
            return this;
        }

        /**
         * Crea las opciones del buffer para el {@link MqttGateway}
         *
         * @return las opciones de conexion
         */
        public DisconnectedBufferOptions createBufferOptions() {
            DisconnectedBufferOptions disconnectedBufferOptions = new DisconnectedBufferOptions();
            disconnectedBufferOptions.setBufferEnabled(bufferEnabled);
            disconnectedBufferOptions.setPersistBuffer(persistBuffer);
            disconnectedBufferOptions.setBufferSize(bufferSize);
            disconnectedBufferOptions.setDeleteOldestMessages(deleteOldestMessages);
            return disconnectedBufferOptions;
        }
    }

    /**
     * Functional Builder Pattern para construir un {@link MqttSubscriberOptions}
     *
     * @author daniel.carvajal
     */
    public static class SubscriberOptionsBuilder {
        public int qos;
        public String topicFilter;
        public Object userContext;
        public IMqttActionListener callback;
        public IMqttMessageListener messageListener;

        /**
         * Consolida todos los parametros del {@link SubscriberOptionsBuilder}
         *
         * @param builder {@link Consumer} para setear los parametros
         * @return la instancia del {@link SubscriberOptionsBuilder}
         */
        public SubscriberOptionsBuilder with(Consumer<SubscriberOptionsBuilder> builder) {
            builder.accept(this);
            return this;
        }

        /**
         * Crea las opciones de subscripcion para el {@link MqttGateway}
         *
         * @return las opciones de subscripcion
         */
        public MqttSubscriberOptions createSubcriberOptions() {
            MqttSubscriberOption subscriberOption = new MqttSubscriberOption();
            //TODO add 1..n options
            subscriberOption.setIndex(0); // default option

            subscriberOption.setQos(qos);
            subscriberOption.setTopicFilter(topicFilter);
            subscriberOption.setUserContext(userContext);
            subscriberOption.setCallback(callback);
            subscriberOption.setMessageListener(messageListener);

            //TODO add 1..n options
            MqttSubscriberOptions subscriberOptions = new MqttSubscriberOptions();
            subscriberOptions.addOption(subscriberOption);
            //
            return subscriberOptions;
        }
    }

    /**
     * Functional Builder Pattern para construir un {@link PublisherOptionsBuilder}
     *
     * @author daniel.carvajal
     */
    public static class PublisherOptionsBuilder {
        public int qos;
        public String topicFilter;
        public boolean retained;
        public Object userContext;
        public IMqttActionListener callback;
        public IMqttMessageListener messageListener;

        /**
         * Consolida todos los parametros del {@link PublisherOptionsBuilder}
         *
         * @param builder {@link Consumer} para setear los parametros
         * @return la instancia del {@link PublisherOptionsBuilder}
         */
        public PublisherOptionsBuilder with(Consumer<PublisherOptionsBuilder> builder) {
            builder.accept(this);
            return this;
        }

        /**
         * Crea las opciones de publicacion para el {@link MqttGateway}
         *
         * @return las opciones de publicacion
         */
        public MqttPublisherOptions createPublisherOptions() {
            MqttPublisherOption publisherOption = new MqttPublisherOption();
            //TODO add 1..n options
            publisherOption.setIndex(0); // default option

            publisherOption.setQos(qos);
            publisherOption.setTopicFilter(topicFilter);
            publisherOption.setRetained(retained);
            publisherOption.setUserContext(userContext);
            publisherOption.setCallback(callback);
            publisherOption.setMessageListener(messageListener);

            //TODO add 1..n options
            MqttPublisherOptions publisherOptions = new MqttPublisherOptions();
            publisherOptions.addOption(publisherOption);
            //
            return publisherOptions;
        }
    }

    /**
     * Thread encargado de realizar el reintento de la conexion al broker si no hay alguna disponble.
     * Este, se asegura que este conectado y luego ejecuta el metodo {@link MqttGateway#subscriberOptions}
     * para asegurar la consitencia
     *
     * @author daniel.carvajal
     */
    public static class RetryConnection extends Thread {

        private static RetryConnection retryConnection;

        private static final Logger logger = LoggerFactory.getLogger(RetryConnection.class);

        private MqttGateway mqttGateway;

        /**
         * Singleton para el {@link RetryConnection}
         *
         * @return nueva instancia del la clase
         */
        public static RetryConnection getInstance(MqttGateway mqttGateway) {
            if (retryConnection == null) {
                mqttGateway = mqttGateway;
                retryConnection = new RetryConnection(mqttGateway);

            }
            return retryConnection;
        }

        /**
         * Constructor que recibe un {@link MqttGateway} para realizar el proceso de conexion
         *
         * @param mqttGateway
         */
        private RetryConnection(MqttGateway mqttGateway){
            this.mqttGateway = mqttGateway;
        }

        /**
         * Metodo que ejecuta la tarea de conexion con el broker y la subcripcion de los topicos
         */
        @Override
        public void run() {
            while (!mqttGateway.isConnected()) {
                try {
                    mqttGateway.saveAttach();

                } catch (Exception e1) {
                    logger.error("[{}] Se ha producido un error al conectar con el server [{}]. Reintentando conexion!!!",
                            MqttGateway.class.getSimpleName(), mqttGateway.getMqttAsyncClient().getServerURI());
                    logger.error(e1.getMessage(), e1);

                    try {
                        mqttGateway.getMqttAsyncClient().close(true);
                        //
                    } catch (MqttException e2) {
                        logger.error(e2.getMessage(), e2);
                    }
                    try {
                        Thread.sleep(5000);

                    } catch (InterruptedException e3) {
                        logger.error(e3.getMessage(), e3);
                    }
                }
            } // end while
            logger.info("[{}] Se ha conectado correctamente el server [{}]!!!",
                    MqttGateway.class.getSimpleName(), mqttGateway.getMqttAsyncClient().getServerURI());

            try {
                mqttGateway.subscribeWithOptions();
            }catch (MqttException e4){
                    logger.error(e4.getMessage(), e4);
            }
        }
    }// end class
}
