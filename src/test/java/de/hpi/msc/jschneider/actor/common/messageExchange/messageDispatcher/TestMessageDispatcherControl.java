package de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher;

import akka.actor.ActorRef;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.actor.ActorTestCase;
import de.hpi.msc.jschneider.actor.TestNode;
import de.hpi.msc.jschneider.actor.common.MockMessage;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMessageDispatcherControl extends ActorTestCase
{
    private TestNode remoteNode;
    private TestProbe localMessageProxy;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        remoteNode = spawnNode("remote");
        localMessageProxy = TestProbe.apply("MessageProxy", localNode.getActorSystem());
    }

    private MessageDispatcherModel dummyModel()
    {
        return MessageDispatcherModel.builder()
                                     .selfProvider(() -> self.ref())
                                     .senderProvider(ActorRef::noSender)
                                     .messageDispatcherProvider(() -> self.ref())
                                     .childFactory(props -> localMessageProxy.ref())
                                     .watchActorCallback(actorRef ->
                                                         {
                                                         })
                                     .build();
    }

    private MessageDispatcherModel modelWithActualChildFactory()
    {
        return MessageDispatcherModel.builder()
                                     .selfProvider(() -> self.ref())
                                     .senderProvider(ActorRef::noSender)
                                     .messageDispatcherProvider(() -> self.ref())
                                     .childFactory(localNode.getActorSystem()::actorOf)
                                     .watchActorCallback(actorRef ->
                                                         {
                                                         })
                                     .build();
    }

    public void testInitialize()
    {
        val control = new MessageDispatcherControl(dummyModel());

        assertThat(control.getModel().getMessageDispatchers().size()).isEqualTo(1);
        assertThat(control.getModel().getMessageDispatchers().get(localNode.rootPath())).isEqualTo(self.ref());
        assertThat(control.getModel().getMessageProxies().size()).isEqualTo(0);
    }

    public void testGetNewMessageProxy()
    {
        val control = new MessageDispatcherControl(dummyModel());
        val messageProxy = control.getMessageProxy(MockMessage.builder()
                                                              .sender(self.ref())
                                                              .receiver(self.ref())
                                                              .build());

        assertThat(messageProxy).isNotNull();
        assertThat(messageProxy).isEqualTo(localMessageProxy.ref());
        assertThat(control.getModel().getMessageProxies().size()).isEqualTo(1);
    }

    public void testGetExistingMessageProxy()
    {
        val model = dummyModel();
        model.getMessageProxies().put(localNode.rootPath(), localMessageProxy.ref());

        val control = new MessageDispatcherControl(model);
        val messageProxy = control.getMessageProxy(MockMessage.builder()
                                                              .sender(self.ref())
                                                              .receiver(self.ref())
                                                              .build());

        assertThat(messageProxy).isNotNull();
        assertThat(messageProxy).isEqualTo(localMessageProxy.ref());
        assertThat(control.getModel().getMessageProxies().size()).isEqualTo(1);
    }

    public void testAddRemoteMessageDispatcher()
    {
        val control = new MessageDispatcherControl(dummyModel());
        val addRemoteDispatcher = MessageDispatcherMessages.AddMessageDispatchersMessage.builder()
                                                                                        .sender(remoteNode.getMessageDispatcher().ref())
                                                                                        .receiver(self.ref())
                                                                                        .messageDispatchers(new ActorRef[]{remoteNode.getMessageDispatcher().ref()})
                                                                                        .build();

        control.onAddMessageDispatchers(addRemoteDispatcher);

        assertThat(control.getModel().getMessageDispatchers().size()).isEqualTo(2);
        assertThat(control.getModel().getMessageDispatchers().get(remoteNode.rootPath())).isEqualTo(remoteNode.getMessageDispatcher().ref());
    }

    public void testGetMessageProxyIsIndifferentOfDirection()
    {
        val control = new MessageDispatcherControl(modelWithActualChildFactory());
        control.getModel().getMessageDispatchers().put(remoteNode.rootPath(), remoteNode.getMessageDispatcher().ref());

        val localToRemoteMessage = MockMessage.builder()
                                              .sender(self.ref())
                                              .receiver(remoteNode.getMessageDispatcher().ref())
                                              .build();
        val remoteToLocalMessage = MockMessage.builder()
                                              .sender(remoteNode.getMessageDispatcher().ref())
                                              .receiver(self.ref())
                                              .build();

        val messageProxy1 = control.getMessageProxy(localToRemoteMessage);
        assertThat(messageProxy1).isNotNull();
        assertThat(messageProxy1).isNotEqualTo(ActorRef.noSender());

        val messageProxy2 = control.getMessageProxy(remoteToLocalMessage);
        assertThat(messageProxy2).isNotNull();
        assertThat(messageProxy2).isNotEqualTo(ActorRef.noSender());

        assertThat(messageProxy1).isEqualTo(messageProxy2);
    }

    public void testForwardMessage()
    {
        val control = new MessageDispatcherControl(dummyModel());
        val message = MockMessage.builder()
                                 .sender(self.ref())
                                 .receiver(self.ref())
                                 .build();
        control.onMessage(message);
        localMessageProxy.expectMsg(message);
    }
}
