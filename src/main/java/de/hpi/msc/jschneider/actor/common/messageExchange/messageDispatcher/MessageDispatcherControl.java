package de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.actor.common.AbstractActorControl;
import de.hpi.msc.jschneider.actor.common.Message;
import de.hpi.msc.jschneider.actor.common.messageExchange.messageProxy.MessageProxy;
import lombok.val;
import lombok.var;

public class MessageDispatcherControl extends AbstractActorControl<MessageDispatcherModel>
{
    public MessageDispatcherControl(MessageDispatcherModel model)
    {
        super(model);
        getModel().getMessageDispatchers().put(getSelf().path().root(), getSelf());
    }

    public void onAddMessageDispatchers(MessageDispatcherMessages.AddMessageDispatchersMessage message)
    {
        for (val messageDispatcher : message.getMessageDispatchers())
        {
            getModel().getMessageDispatchers().putIfAbsent(messageDispatcher.path().root(), messageDispatcher);
        }
    }

    public void onMessage(Message message)
    {
        val proxy = getMessageProxy(message);
        proxy.tell(message, getSender());
    }

    public ActorRef getMessageProxy(Message message)
    {
        var rootPath = message.getReceiver().path().root();
        if (rootPath == getSelf().path().root())
        {
            rootPath = message.getSender().path().root();
        }

        return getMessageProxy(rootPath);
    }

    private ActorRef getMessageProxy(RootActorPath remoteActorSystemPath)
    {
        var messageProxy = getModel().getMessageProxies().get(remoteActorSystemPath);
        if (messageProxy == null)
        {
            messageProxy = createMessageProxy(remoteActorSystemPath);
            getModel().getMessageProxies().put(remoteActorSystemPath, messageProxy);
        }

        return messageProxy;
    }

    private ActorRef createMessageProxy(RootActorPath remoteActorSystemPath)
    {
        val remoteMessageDispatcher = getModel().getMessageDispatchers().get(remoteActorSystemPath);
        if (remoteMessageDispatcher == null)
        {
            getLog().error(String.format("Unable to get MessageProxy for remote actor system at %1$s!", remoteActorSystemPath));
            return ActorRef.noSender();
        }

        return createChild(MessageProxy.props(remoteMessageDispatcher));
    }
}
