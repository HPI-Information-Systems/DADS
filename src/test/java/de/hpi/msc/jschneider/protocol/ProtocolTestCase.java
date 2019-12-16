package de.hpi.msc.jschneider.protocol;

import akka.actor.ActorRef;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherMessages;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.MockDataSink;
import junit.framework.TestCase;
import lombok.val;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class ProtocolTestCase extends TestCase
{
    private final List<TestProcessor> processors = new ArrayList<>();
    protected TestProcessor localProcessor;
    protected TestProbe self;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        localProcessor = createProcessor("local", getProcessorProtocols());
        localProcessor.setMaster(true);
        self = localProcessor.createActor("self");
    }

    protected abstract ProtocolType[] getProcessorProtocols();

    @Override
    public void tearDown() throws Exception
    {
        super.tearDown();
        for (val processor : processors)
        {
            processor.terminate();
        }
    }

    protected TestProcessor createProcessor(String name)
    {
        return createProcessor(name, getProcessorProtocols());
    }

    protected TestProcessor createProcessor(String name, ProtocolType... protocols)
    {
        val processor = TestProcessor.create(name);

        for (val protocolType : protocols)
        {
            TestProtocol.create(processor, protocolType);
        }
        processors.add(processor);
        return processor;
    }

    protected <TModel extends ProtocolParticipantModel> TModel finalizeModel(TModel model)
    {
        model.setSelfProvider(() -> self.ref());
        model.setSenderProvider(ActorRef::noSender);
        model.setProcessorProvider(() -> processors.toArray(new Processor[0]));
        model.setMaximumMessageSizeProvider(() -> 1024 * 1024 * 10L); // 10 MiB
        model.setWatchActorCallback(subject ->
                                    {
                                    });
        model.setUnwatchActorCallback(subject ->
                                      {
                                      });
        model.setChildFactory((props, name) -> ActorRef.noSender());

        return model;
    }

    protected <TModel extends ProtocolParticipantModel, TControl extends ProtocolParticipantControl<TModel>> PartialFunction<Object, BoxedUnit> messageInterface(TControl control)
    {
        val receiveBuilder = new ImprovedReceiveBuilder();
        control.complementReceiveBuilder(receiveBuilder);

        return receiveBuilder.build().onMessage();
    }

    protected MessageExchangeMessages.MessageCompletedMessage assertThatMessageIsCompleted(MessageExchangeMessages.MessageExchangeMessage message)
    {
        val localMessageDispatcher = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange);
        val completedMessage = localMessageDispatcher.expectMsgClass(MessageExchangeMessages.MessageCompletedMessage.class);

        assertThat(completedMessage.getCompletedMessageId()).isEqualTo(message.getId());

        return completedMessage;
    }

    protected <TEvent extends MessageExchangeMessages.RedirectableMessage> TEvent expectEvent(Class<TEvent> eventType)
    {
        val localMessageDispatcher = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange);
        return localMessageDispatcher.expectMsgClass(eventType);
    }

    protected MessageExchangeMessages.MessageExchangeMessage expectEventSubscription(Class<?> eventType)
    {
        val localMessageDispatcher = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange);
        val subscriptionMessage = localMessageDispatcher.expectMsgClass(EventDispatcherMessages.SubscribeToEventMessage.class);

        assertThat(subscriptionMessage.getEventType()).isEqualTo(eventType);

        return subscriptionMessage;
    }

    protected MockDataSink acceptDataTransfer(TestProbe dataReceiver, TestProbe dataSender, DataTransferMessages.InitializeDataTransferMessage initializationMessage)
    {
        val mockSink = new MockDataSink();
        val operationId = initializationMessage.getOperationId();

        while (true)
        {
            val requestNextPart = DataTransferMessages.RequestNextDataPartMessage.builder()
                                                                                 .sender(dataReceiver.ref())
                                                                                 .receiver(dataSender.ref())
                                                                                 .operationId(operationId)
                                                                                 .build();
            dataSender.ref().tell(requestNextPart, dataReceiver.ref());

            val partMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.DataPartMessage.class);
            mockSink.write(partMessage.getPart());

            if (partMessage.isLastPart())
            {
                mockSink.close();
                break;
            }
        }

        return mockSink;
    }
}
