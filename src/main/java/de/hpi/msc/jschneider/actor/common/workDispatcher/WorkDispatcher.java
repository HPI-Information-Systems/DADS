package de.hpi.msc.jschneider.actor.common.workDispatcher;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import de.hpi.msc.jschneider.actor.common.AbstractReapedActor;
import de.hpi.msc.jschneider.actor.common.CompletableMessage;
import de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher.MessageDispatcher;

public class WorkDispatcher extends AbstractReapedActor<WorkDispatcherModel, WorkDispatcherControl>
{
    private static final String NAME = "WorkDispatcher";
    private static ActorRef singletonInstance;

    public static void initializeSingleton(ActorSystem actorSystem) throws Exception
    {
        if (singletonInstance != null)
        {
            throw new Exception("The WorkDispatcher has already been initialized!");
        }

        singletonInstance = actorSystem.actorOf(Props.create(WorkDispatcher.class), NAME);
    }

    public static ActorRef getLocalSingleton()
    {
        return singletonInstance;
    }

    private WorkDispatcher()
    {
        setModel(createModel());
        setControl(createControl(model()));
    }

    private WorkDispatcherModel createModel()
    {
        return WorkDispatcherModel.builder()
                                  .selfProvider(this::self)
                                  .senderProvider(this::sender)
                                  .messageDispatcherProvider(MessageDispatcher::getLocalSingleton)
                                  .childFactory(context()::actorOf)
                                  .watchActorCallback(context()::watch)
                                  .build();
    }

    private WorkDispatcherControl createControl(WorkDispatcherModel model)
    {
        return new WorkDispatcherControl(model);
    }

    @Override
    public Receive createReceive()
    {
        return defaultReceiveBuilder().match(WorkDispatcherMessages.AcknowledgeRegistrationMessage.class, control()::onAcknowledgeRegistration)
                                      .match(WorkDispatcherMessages.RegisterAtMasterMessage.class, control()::onRegisterAtMaster)
                                      .match(CompletableMessage.class, control()::send)
                                      .build();
    }
}
