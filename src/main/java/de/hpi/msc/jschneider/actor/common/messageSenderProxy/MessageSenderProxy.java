package de.hpi.msc.jschneider.actor.common.messageSenderProxy;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import de.hpi.msc.jschneider.actor.common.AbstractActor;
import de.hpi.msc.jschneider.actor.common.Message;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MessageSenderProxy extends AbstractActor<MessageSenderProxyModel, MessageSenderProxyControl>
{
    private static final String NAME = "MessageSenderProxy";
    private static ActorRef globalInstance;
    private static final Lock globalInstanceLock = new ReentrantLock();

    public static ActorRef createIn(ActorSystem actorSystem)
    {
        return actorSystem.actorOf(Props.create(MessageSenderProxy.class), NAME);
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
        return defaultReceiveBuilder().match(MessageSenderProxyMessages.AddMessageReceiverMessage.class, control()::onAddMessageReceiver)
                                      .match(Message.class, control()::onMessage)
                                      .build();
    }
}
