package de.hpi.msc.jschneider.actor.common.messageSenderProxy;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import de.hpi.msc.jschneider.actor.common.AbstractActor;
import de.hpi.msc.jschneider.actor.common.Message;
import de.hpi.msc.jschneider.actor.utility.actorPool.ActorPool;
import de.hpi.msc.jschneider.actor.utility.actorPool.RoundRobinActorPool;
import lombok.var;

import java.util.concurrent.atomic.AtomicInteger;

public class MessageSenderProxy extends AbstractActor<MessageSenderProxyModel, MessageSenderProxyControl>
{
    private static final String NAME = "MessageSenderProxy";
    private static ActorPool instancePool;

    public static void initializePool(ActorSystem actorSystem, int poolSize) throws Exception
    {
        if (instancePool != null)
        {
            throw new Exception("The global MessageSenderProxy pool has already been initialized!");
        }

        instancePool = new RoundRobinActorPool(NAME, poolSize, actorName -> actorSystem.actorOf(Props.create(MessageSenderProxy.class), actorName));
    }

    public static ActorRef getLocalActor()
    {
        return instancePool.getActor();
    }

    @Override
    protected MessageSenderProxyModel createModel()
    {
        return MessageSenderProxyModel.builder()
                                      .selfProvider(this::self)
                                      .senderProvider(this::sender)
                                      .messageSenderProxyProvider(this::self)
                                      .childFactory(context()::actorOf)
                                      .watchActorCallback(context()::watch)
                                      .build();
    }

    @Override
    protected MessageSenderProxyControl createControl(MessageSenderProxyModel model)
    {
        return new MessageSenderProxyControl(model);
    }

    @Override
    public Receive createReceive()
    {
        return defaultReceiveBuilder().match(MessageSenderProxyMessages.AddMessageReceiverPoolMessage.class, control()::onAddMessageReceiver)
                                      .match(Message.class, control()::onMessage)
                                      .build();
    }
}
