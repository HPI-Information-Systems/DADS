package de.hpi.msc.jschneider.protocol.messageExchange.messageDispatcher;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.messageExchange.messageProxy.MessageProxyControl;
import de.hpi.msc.jschneider.protocol.messageExchange.messageProxy.MessageProxyModel;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;
import lombok.var;

import java.util.Optional;

public class MessageDispatcherControl extends AbstractProtocolParticipantControl<MessageDispatcherModel>
{
    public MessageDispatcherControl(MessageDispatcherModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp)
                      .match(MessageExchangeMessages.MessageExchangeMessage.class, this::onMessage);
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {

    }

    private void onMessage(MessageExchangeMessages.MessageExchangeMessage message)
    {
        val proxy = getMessageProxy(message);
        proxy.ifPresent(actorRef -> actorRef.tell(message, message.getSender()));
    }

    private Optional<ActorRef> getMessageProxy(MessageExchangeMessages.MessageExchangeMessage message)
    {
        var rootPath = message.getReceiver().path().root();
        if (rootPath == getModel().getSelf().path().root())
        {
            rootPath = message.getSender().path().root();
        }

        return getOrCreateMessageProxy(rootPath);
    }

    private Optional<ActorRef> getOrCreateMessageProxy(RootActorPath actorSystem)
    {
        var messageProxy = Optional.ofNullable(getModel().getMessageProxies().get(actorSystem));
        if (!messageProxy.isPresent())
        {
            messageProxy = tryCreateMessageProxy(actorSystem);
            messageProxy.ifPresent(actorRef -> getModel().getMessageProxies().put(actorSystem, actorRef));
        }

        return messageProxy;
    }

    private Optional<ActorRef> tryCreateMessageProxy(RootActorPath actorSystem)
    {
        val remoteMessageDispatcher = getProtocol(actorSystem, ProtocolType.MessageExchange);
        if (!remoteMessageDispatcher.isPresent())
        {
            getLog().error(String.format("Unable to get the MessageExchange root actor for (remote) actor system at \"%1$s\"!",
                                         actorSystem));

            return Optional.empty();
        }

        val model = MessageProxyModel.builder()
                                     .messageDispatcher(remoteMessageDispatcher.get().getRootActor())
                                     .build();

        val control = new MessageProxyControl(model);

        return trySpawnChild(ProtocolParticipant.props(control));
    }
}
