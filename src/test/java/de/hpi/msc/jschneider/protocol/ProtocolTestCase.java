package de.hpi.msc.jschneider.protocol;

import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherMessages;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.MockDataSink;
import lombok.val;

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
