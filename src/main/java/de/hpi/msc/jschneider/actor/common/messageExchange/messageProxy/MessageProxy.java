package de.hpi.msc.jschneider.actor.common.messageExchange.messageProxy;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.msc.jschneider.actor.common.AbstractActor;
import de.hpi.msc.jschneider.actor.common.CompletableMessage;
import de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher.MessageDispatcher;

public class MessageProxy extends AbstractActor<MessageProxyModel, MessageProxyControl>
{
    public static Props props(ActorRef remoteMessageDispatcher)
    {
        return Props.create(MessageProxy.class, () -> new MessageProxy(remoteMessageDispatcher));
    }

    private MessageProxy(ActorRef remoteMessageDispatcher)
    {
        setModel(createModel(remoteMessageDispatcher));
        setControl(createControl(model()));
    }

    private MessageProxyModel createModel(ActorRef remoteMessageDispatcher)
    {
        return MessageProxyModel.builder()
                                .selfProvider(this::self)
                                .senderProvider(this::sender)
                                .messageDispatcherProvider(MessageDispatcher::getLocalSingleton)
                                .childFactory(context()::actorOf)
                                .watchActorCallback(context()::watch)
                                .remoteMessageDispatcher(remoteMessageDispatcher)
                                .build();
    }

    private MessageProxyControl createControl(MessageProxyModel model)
    {
        return new MessageProxyControl(model);
    }

    @Override
    public Receive createReceive()
    {
        return defaultReceiveBuilder().match(CompletableMessage.class, control()::onMessage)
                                      .match(MessageProxyMessages.MessageCompletedMessage.class, control()::onMessageCompleted)
                                      .build();
    }
}
