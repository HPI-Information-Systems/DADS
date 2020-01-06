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
import de.hpi.msc.jschneider.utility.dataTransfer.sink.PrimitiveMatrixSink;
import junit.framework.TestCase;
import lombok.val;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseTestCase extends TestCase
{
    private final List<TestProcessor> processors = new ArrayList<>();

    protected TestProcessor createMaster()
    {
        return createMaster(getProcessorProtocols());
    }

    protected TestProcessor createMaster(ProtocolType... protocolTypes)
    {
        val numberOfMasters = processors.stream().filter(TestProcessor::isMaster).count();
        return createProcessor(String.format("Master-%1$d", numberOfMasters), true, protocolTypes);
    }

    protected TestProcessor createSlave()
    {
        return createSlave(getProcessorProtocols());
    }

    protected TestProcessor createSlave(ProtocolType... protocolTypes)
    {
        val numberOfSlaves = processors.stream().filter(processor -> !processor.isMaster()).count();
        return createProcessor(String.format("Slave-%1$d", numberOfSlaves), false, protocolTypes);
    }

    protected TestProcessor createProcessor(String name, boolean isMaster)
    {
        return createProcessor(name, isMaster, getProcessorProtocols());
    }

    protected abstract ProtocolType[] getProcessorProtocols();

    protected TestProcessor createProcessor(String name, boolean isMaster, ProtocolType... protocols)
    {
        val processor = TestProcessor.create(name);
        processor.setMaster(isMaster);
        for (val protocolType : protocols)
        {
            TestProtocol.create(processor, protocolType);
        }
        processors.add(processor);

        return processor;
    }

    protected <TModel extends ProtocolParticipantModel> TModel finalizeModel(TModel model, TestProbe self)
    {
        model.setSelfProvider(self::ref);
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

    protected <TModel extends ProtocolParticipantModel, TControl extends ProtocolParticipantControl<TModel>> PartialFunction<Object, BoxedUnit> createMessageInterface(TControl control)
    {
        val receiveBuilder = new ImprovedReceiveBuilder();
        control.complementReceiveBuilder(receiveBuilder);

        return receiveBuilder.build().onMessage();
    }

    protected MessageExchangeMessages.MessageCompletedMessage assertThatMessageIsCompleted(MessageExchangeMessages.MessageExchangeMessage message, TestProcessor processor)
    {
        val messageDispatcher = processor.getProtocolRootActor(ProtocolType.MessageExchange);
        val completedMessage = messageDispatcher.expectMsgClass(MessageExchangeMessages.MessageCompletedMessage.class);

        assertThat(completedMessage.getCompletedMessageId()).isEqualTo(message.getId());

        return completedMessage;
    }

    protected EventDispatcherMessages.SubscribeToEventMessage assertEventSubscription(Class<?> eventType, TestProcessor processor)
    {
        val messageDispatcher = processor.getProtocolRootActor(ProtocolType.MessageExchange);
        val subscription = messageDispatcher.expectMsgClass(EventDispatcherMessages.SubscribeToEventMessage.class);

        assertThat(subscription.getEventType()).isEqualTo(eventType);

        return subscription;
    }

    protected <TEvent extends MessageExchangeMessages.RedirectableMessage> TEvent expectEvent(Class<TEvent> eventType, TestProcessor processor)
    {
        val messageDispatcher = processor.getProtocolRootActor(ProtocolType.MessageExchange);
        return messageDispatcher.expectMsgClass(eventType);
    }

    protected PrimitiveMatrixSink performDataTransfer(TestProbe dataReceiver, PartialFunction<Object, BoxedUnit> dataReceiverMessageInterface,
                                                      TestProbe dataSender, PartialFunction<Object, BoxedUnit> dataSenderMessageInterface,
                                                      DataTransferMessages.InitializeDataTransferMessage initializeDataTransferMessage,
                                                      boolean expectFinalMessageCompletion)
    {
        val receiverProcessor = processors.stream().filter(processor -> processor.getRootPath().equals(dataReceiver.ref().path().root())).findFirst();
        assert receiverProcessor.isPresent() : "Unable to find receiverProcessor!";

        val senderProcessor = processors.stream().filter(processor -> processor.getRootPath().equals(dataSender.ref().path().root())).findFirst();
        assert senderProcessor.isPresent() : "Unable to find senderProcessor!";

        val sink = new PrimitiveMatrixSink();
        val operationId = initializeDataTransferMessage.getOperationId();

        MessageExchangeMessages.MessageExchangeMessage nextMessageToCompleteOnReceiver = initializeDataTransferMessage;
        dataReceiverMessageInterface.apply(initializeDataTransferMessage);

        while (true)
        {
            val requestNextPart = receiverProcessor.get().getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
            assertThat(requestNextPart.getOperationId()).isEqualTo(operationId);
            dataSenderMessageInterface.apply(requestNextPart);
            assertThatMessageIsCompleted(nextMessageToCompleteOnReceiver, receiverProcessor.get());

            val part = senderProcessor.get().getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.DataPartMessage.class);
            sink.write(part.getPart());
            dataReceiverMessageInterface.apply(part);
            assertThatMessageIsCompleted(requestNextPart, senderProcessor.get());

            nextMessageToCompleteOnReceiver = part;

            if (!part.isLastPart())
            {
                continue;
            }

            if (expectFinalMessageCompletion)
            {
                assertThatMessageIsCompleted(part, receiverProcessor.get());
            }

            sink.close();
            break;
        }

        return sink;
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();

        for (val processor : processors)
        {
            processor.terminate();
        }
        processors.clear();
    }
}
