package de.hpi.msc.jschneider.actor.common.messageReceiverProxy;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import de.hpi.msc.jschneider.actor.common.AbstractActor;
import de.hpi.msc.jschneider.actor.common.Message;
import de.hpi.msc.jschneider.actor.common.messageSenderProxy.MessageSenderProxy;
import de.hpi.msc.jschneider.actor.utility.actorPool.ActorPool;
import de.hpi.msc.jschneider.actor.utility.actorPool.RoundRobinActorPool;

public class MessageReceiverProxy extends AbstractActor<MessageReceiverProxyModel, MessageReceiverProxyControl>
{
    private static final String NAME = "MessageReceiverProxy";
    private static ActorPool instancePool;

    public static void initializePool(ActorSystem actorSystem, int poolSize) throws Exception
    {
        if (instancePool != null)
        {
            throw new Exception("The global MessageReceiverProxy pool has already been initialized!");
        }

        instancePool = new RoundRobinActorPool(NAME, poolSize, actorName -> actorSystem.actorOf(Props.create(MessageReceiverProxy.class), actorName));
    }

    public static ActorRef getLocalActor()
    {
        return instancePool.getActor();
    }

    public static ActorRef[] getAllLocalActors()
    {
        return instancePool.getActors();
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
