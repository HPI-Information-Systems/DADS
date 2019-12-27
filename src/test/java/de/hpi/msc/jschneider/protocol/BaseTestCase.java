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

    protected <TModel extends ProtocolParticipantModel, TControl extends ProtocolParticipantControl<TModel>> PartialFunction<Object, BoxedUnit> messageInterface(TControl control)
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

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();

        for (val processor : processors)
        {
            processor.terminate();
        }
    }
}
