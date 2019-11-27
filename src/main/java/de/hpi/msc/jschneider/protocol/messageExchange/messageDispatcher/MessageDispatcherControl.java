package de.hpi.msc.jschneider.protocol.messageExchange.messageDispatcher;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher.MessageDispatcher;
import de.hpi.msc.jschneider.actor.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.messageExchange.messageProxy.MessageProxyControl;
import de.hpi.msc.jschneider.protocol.messageExchange.messageProxy.MessageProxyModel;
import lombok.val;
import lombok.var;

public class MessageDispatcherControl extends AbstractProtocolParticipantControl<MessageDispatcherModel>
{
    public MessageDispatcherControl(MessageDispatcherModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(MessageExchangeMessages.MessageExchangeMessage.class, this::onCompletableMessage);
    }

    private void onCompletableMessage(MessageExchangeMessages.MessageExchangeMessage message)
    {
        val proxy = getMessageProxy(message);
        proxy.tell(message, message.getSender());
    }

    private ActorRef getMessageProxy(MessageExchangeMessages.MessageExchangeMessage message)
    {
        var rootPath = message.getReceiver().path().root();
        if (rootPath == getModel().getSelf().path().root())
        {
            rootPath = message.getSender().path().root();
        }

        return getOrCreateMessageProxy(rootPath);
    }

    private ActorRef getOrCreateMessageProxy(RootActorPath actorSystem)
    {
        var messageProxy = getModel().getMessageProxies().get(actorSystem);
        if (messageProxy == null)
        {
            messageProxy = createMessageProxy(actorSystem);
            getModel().getMessageProxies().put(actorSystem, messageProxy);
        }

        return messageProxy;
    }

    private ActorRef createMessageProxy(RootActorPath actorSystem)
    {
        val remoteMessageDispatcher = getModel().getMessageDispatchers().get(actorSystem);
        if (remoteMessageDispatcher == null)
        {
            getLog().error(String.format("Unable to get the %1$s for (remote) actor system at \"%2$s\"!",
                                         MessageDispatcher.class.getName(),
                                         actorSystem));

            return ActorRef.noSender();
        }

        val model = MessageProxyModel.builder()
                                     .messageDispatcher(remoteMessageDispatcher)
                                     .build();

        val control = new MessageProxyControl(model);

        return spawnChild(ProtocolParticipant.props(control));
    }
}
