package de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import de.hpi.msc.jschneider.actor.common.AbstractActor;
import de.hpi.msc.jschneider.actor.common.CompletableMessage;

public class MessageDispatcher extends AbstractActor<MessageDispatcherModel, MessageDispatcherControl>
{
    private static final String NAME = "MessageDispatcher";
    private static ActorRef singletonInstance;

    public static void initializeSingleton(ActorSystem actorSystem) throws Exception
    {
        if (singletonInstance != null)
        {
            throw new Exception("The MessageDispatcher has already been initialized!");
        }

        singletonInstance = actorSystem.actorOf(Props.create(MessageDispatcher.class), NAME);
    }

    public static ActorRef getLocalSingleton()
    {
        return singletonInstance;
    }

    private MessageDispatcher()
    {
        setModel(createModel());
        setControl(createControl(model()));
    }

    private MessageDispatcherModel createModel()
    {
        return MessageDispatcherModel.builder()
                                     .selfProvider(this::self)
                                     .senderProvider(this::sender)
                                     .messageDispatcherProvider(this::self)
                                     .childFactory(context()::actorOf)
                                     .watchActorCallback(context()::watch)
                                     .build();
    }

    private MessageDispatcherControl createControl(MessageDispatcherModel model)
    {
        return new MessageDispatcherControl(model);
    }

    @Override
    public Receive createReceive()
    {
        return defaultReceiveBuilder().match(MessageDispatcherMessages.AddMessageDispatchersMessage.class, control()::onAddMessageDispatchers)
                                      .match(CompletableMessage.class, control()::onMessage)
                                      .build();
    }
}
