package de.hpi.msc.jschneider.protocol;

import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherMessages;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;

public abstract class ProtocolTestCase extends BaseTestCase
{
    protected TestProcessor localProcessor;
    protected TestProbe self;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        localProcessor = createMaster();
        self = localProcessor.createActor("self");
    }

    protected <TModel extends ProtocolParticipantModel> TModel finalizeModel(TModel model)
    {
        return finalizeModel(model, self);
    }

    protected MessageExchangeMessages.MessageCompletedMessage assertThatMessageIsCompleted(MessageExchangeMessages.MessageExchangeMessage message)
    {
        return assertThatMessageIsCompleted(message, localProcessor);
    }

    protected <TEvent extends MessageExchangeMessages.RedirectableMessage> TEvent expectEvent(Class<TEvent> eventType)
    {
        return expectEvent(eventType, localProcessor);
    }

    protected EventDispatcherMessages.SubscribeToEventMessage assertEventSubscription(Class<?> eventType)
    {
        return assertEventSubscription(eventType, localProcessor);
    }
}
