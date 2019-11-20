package de.hpi.msc.jschneider.actor.common.messageReceiverProxy;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import de.hpi.msc.jschneider.actor.common.AbstractActor;
import de.hpi.msc.jschneider.actor.common.Message;
import de.hpi.msc.jschneider.actor.common.messageSenderProxy.MessageSenderProxy;
import lombok.var;

import java.util.concurrent.atomic.AtomicInteger;

public class MessageReceiverProxy extends AbstractActor<MessageReceiverProxyModel, MessageReceiverProxyControl>
{
    private static final String NAME = "MessageReceiverProxy";
    private static final AtomicInteger nextInstanceIndex = new AtomicInteger();
    private static ActorRef[] instancePool;

    public static void initializePool(ActorSystem actorSystem, int poolSize) throws Exception
    {
        if (instancePool != null)
        {
            throw new Exception("The global MessageReceiverProxy pool has already been initialized!");
        }

        instancePool = new ActorRef[poolSize];
        for (var i = 0; i < poolSize; ++i)
        {
            instancePool[i] = actorSystem.actorOf(Props.create(MessageSenderProxy.class), NAME + i);
        }
    }

    public static ActorRef getLocalActor()
    {
        return instancePool[nextInstanceIndex.getAndIncrement() % instancePool.length];
    }

    public static ActorRef[] getAllLocalActors()
    {
        return instancePool;
    }

    @Override
    protected MessageReceiverProxyModel createModel()
    {
        return MessageReceiverProxyModel.builder()
                                        .selfProvider(this::self)
                                        .senderProvider(this::sender)
                                        .messageSenderProxyProvider(MessageSenderProxy::getLocalActor)
                                        .childFactory(context()::actorOf)
                                        .watchActorCallback(context()::watch)
                                        .build();
    }

    @Override
    protected MessageReceiverProxyControl createControl(MessageReceiverProxyModel model)
    {
        return new MessageReceiverProxyControl(model);
    }

    @Override
    public Receive createReceive()
    {
        return defaultReceiveBuilder().match(Message.class, control()::onMessage)
                                      .build();
    }
}
