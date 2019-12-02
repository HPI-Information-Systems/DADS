package de.hpi.msc.jschneider.protocol;

import akka.actor.ActorRef;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
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
        model.setProcessorProvider(actorSystem ->
                                   {
                                       for (val processor : processors)
                                       {
                                           if (processor.getRootPath().equals(actorSystem))
                                           {
                                               return processor;
                                           }
                                       }

                                       return null;
                                   });
        model.setWatchActorCallback(subject ->
                                    {
                                    });
        model.setUnwatchActorCallback(subject ->
                                      {
                                      });
        model.setChildFactory(props -> ActorRef.noSender());

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
}
