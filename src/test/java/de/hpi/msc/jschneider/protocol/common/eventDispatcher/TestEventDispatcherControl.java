package de.hpi.msc.jschneider.protocol.common.eventDispatcher;

import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.TestMessage;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestEventDispatcherControl extends ProtocolTestCase
{
    private TestProbe localActor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        localActor = localProcessor.createActor("actor");
    }


    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange};
    }

    private EventDispatcherControl<EventDispatcherModel> control()
    {
        return new BaseEventDispatcherControl<>(dummyModel());
    }

    private EventDispatcherModel dummyModel()
    {
        return finalizeModel(BaseEventDispatcherModel.create(TestMessage.class));
    }

    private EventDispatcherMessages.SubscribeToEventMessage subscribeToTestMessage(TestProbe subscriber)
    {
        return EventDispatcherMessages.SubscribeToEventMessage.builder()
                                                              .sender(subscriber.ref())
                                                              .receiver(self.ref())
                                                              .eventType(TestMessage.class)
                                                              .build();
    }

    private EventDispatcherMessages.UnsubscribeFromEventMessage unsubscribeFromTestMessage(TestProbe subscriber)
    {
        return EventDispatcherMessages.UnsubscribeFromEventMessage.builder()
                                                                  .sender(subscriber.ref())
                                                                  .receiver(self.ref())
                                                                  .eventType(TestMessage.class)
                                                                  .build();
    }

    public void testSubscribe()
    {
        val control = control();
        val messageInterface = messageInterface(control);

        val subscribeMessage = subscribeToTestMessage(localActor);
        messageInterface.apply(subscribeMessage);

        assertThat(control.getModel().getEventSubscribers().size()).isOne();
        assertThat(control.getModel().getEventSubscribers().get(TestMessage.class).size()).isOne();
        assertThat(control.getModel().getEventSubscribers().get(TestMessage.class)).contains(localActor.ref());
        assertThatMessageIsCompleted(subscribeMessage);
    }

    public void testSubscribeTwice()
    {
        val control = control();
        val messageInterface = messageInterface(control);

        val firstSubscription = subscribeToTestMessage(localActor);
        messageInterface.apply(firstSubscription);

        assertThat(control.getModel().getEventSubscribers().size()).isOne();
        assertThat(control.getModel().getEventSubscribers().get(TestMessage.class).size()).isOne();
        assertThat(control.getModel().getEventSubscribers().get(TestMessage.class)).contains(localActor.ref());
        assertThatMessageIsCompleted(firstSubscription);

        val secondSubscription = subscribeToTestMessage(localActor);
        messageInterface.apply(secondSubscription);

        assertThat(control.getModel().getEventSubscribers().size()).isOne();
        assertThat(control.getModel().getEventSubscribers().get(TestMessage.class).size()).isOne();
        assertThat(control.getModel().getEventSubscribers().get(TestMessage.class)).contains(localActor.ref());
        assertThatMessageIsCompleted(secondSubscription);
    }

    public void testUnsubscribe()
    {
        val control = control();
        control.getModel().getEventSubscribers().get(TestMessage.class).add(localActor.ref());
        val messageInterface = messageInterface(control);

        val unsubscribeMessage = unsubscribeFromTestMessage(localActor);
        messageInterface.apply(unsubscribeMessage);

        assertThat(control.getModel().getEventSubscribers().size()).isOne();
        assertThat(control.getModel().getEventSubscribers().get(TestMessage.class)).isEmpty();
        assertThatMessageIsCompleted(unsubscribeMessage);
    }

    public void testDispatchEvent()
    {
        val control = control();
        control.getModel().getEventSubscribers().get(TestMessage.class).add(localActor.ref());
        val messageInterface = messageInterface(control);

        val event = TestMessage.builder()
                               .sender(localActor.ref())
                               .receiver(self.ref())
                               .build();
        messageInterface.apply(event);

        val dispatchedEvent = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(TestMessage.class);
        assertThat(dispatchedEvent.getId()).isEqualTo(event.getId());
        assertThat(dispatchedEvent.getReceiver()).isEqualTo(localActor.ref());
        assertThatMessageIsCompleted(event);
    }
}
