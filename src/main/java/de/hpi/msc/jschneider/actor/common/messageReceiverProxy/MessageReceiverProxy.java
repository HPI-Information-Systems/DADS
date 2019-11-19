package de.hpi.msc.jschneider.actor.common.messageReceiverProxy;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import de.hpi.msc.jschneider.actor.common.AbstractActor;
import de.hpi.msc.jschneider.actor.common.Message;
import de.hpi.msc.jschneider.actor.common.messageSenderProxy.MessageSenderProxy;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MessageReceiverProxy extends AbstractActor<MessageReceiverProxyModel, MessageReceiverProxyControl>
{
    private static final String NAME = "MessageReceiverProxy";
    private static ActorRef globalInstance;
    private static final Lock globalInstanceLock = new ReentrantLock();

    public static ActorRef createIn(ActorSystem actorSystem)
    {
        return actorSystem.actorOf(Props.create(MessageReceiverProxy.class), NAME);
    }

    public static ActorRef globalInstance()
    {
        globalInstanceLock.lock();

        try
        {
            return globalInstance;
        }
        finally
        {
            globalInstanceLock.unlock();
        }
    }

    public static void globalInstance(ActorRef instance)
    {
        globalInstanceLock.lock();

        try
        {
            globalInstance = instance;
        }
        finally
        {
            globalInstanceLock.unlock();
        }
    }

    @Override
    protected MessageReceiverProxyModel createModel()
    {
        return MessageReceiverProxyModel.builder()
                                        .selfProvider(this::self)
                                        .senderProvider(this::sender)
                                        .messageSenderProxyProvider(MessageSenderProxy::globalInstance)
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
