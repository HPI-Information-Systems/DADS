package de.hpi.msc.jschneider.actor.common.reaper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import de.hpi.msc.jschneider.actor.common.AbstractActor;
import de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher.MessageDispatcher;

public class Reaper extends AbstractActor<ReaperModel, ReaperControl>
{
    private static final String NAME = "Reaper";
    private static ActorRef singletonInstance;

    public static void initializeSingleton(ActorSystem actorSystem) throws Exception
    {
        if (singletonInstance != null)
        {
            throw new Exception("The Reaper has already been initialized!");
        }

        singletonInstance = actorSystem.actorOf(Props.create(Reaper.class), NAME);
    }

    public static ActorRef getLocalSingleton()
    {
        return singletonInstance;
    }

    private Reaper()
    {
        setModel(createModel());
        setControl(createControl(model()));
    }

    private ReaperModel createModel()
    {
        return ReaperModel.builder()
                          .selfProvider(this::self)
                          .senderProvider(this::sender)
                          .messageDispatcherProvider(MessageDispatcher::getLocalSingleton)
                          .childFactory(context()::actorOf)
                          .watchActorCallback(context()::watch)
                          .terminateSystemCallback(context().system()::terminate)
                          .build();
    }

    private ReaperControl createControl(ReaperModel model)
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
