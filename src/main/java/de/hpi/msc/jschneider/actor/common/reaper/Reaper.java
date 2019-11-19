package de.hpi.msc.jschneider.actor.common.reaper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import de.hpi.msc.jschneider.actor.common.AbstractActor;
import de.hpi.msc.jschneider.actor.common.messageSenderProxy.MessageSenderProxy;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Reaper extends AbstractActor<ReaperModel, ReaperControl>
{
    private static final String NAME = "MessageReceiverProxy";
    private static ActorRef globalInstance;
    private static final Lock globalInstanceLock = new ReentrantLock();

    public static ActorRef createIn(ActorSystem actorSystem)
    {
        return actorSystem.actorOf(Props.create(Reaper.class), NAME);
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
    protected ReaperModel createModel()
    {
        return ReaperModel.builder()
                          .selfProvider(this::self)
                          .senderProvider(this::sender)
                          .messageSenderProxyProvider(MessageSenderProxy::globalInstance)
                          .childFactory(context()::actorOf)
                          .watchActorCallback(context()::watch)
                          .terminateSystemCallback(context().system()::terminate)
                          .build();
    }

    @Override
    protected ReaperControl createControl(ReaperModel model)
    {
        return new ReaperControl(model);
    }

    @Override
    public Receive createReceive()
    {
        return defaultReceiveBuilder().match(ReaperMessages.WatchMeMessage.class, control()::onWatchMe)
                                      .build();
    }
}
