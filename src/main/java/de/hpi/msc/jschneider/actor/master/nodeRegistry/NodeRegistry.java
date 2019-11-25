package de.hpi.msc.jschneider.actor.master.nodeRegistry;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import de.hpi.msc.jschneider.actor.common.AbstractActor;
import de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher.MessageDispatcher;
import de.hpi.msc.jschneider.actor.common.workDispatcher.WorkDispatcher;

public class NodeRegistry extends AbstractActor<NodeRegistryModel, NodeRegistryControl>
{
    public static final String NAME = "NodeRegistry";
    private static ActorRef singletonInstance;

    public static void initializeSingleton(ActorSystem actorSystem) throws Exception
    {
        if (singletonInstance != null)
        {
            throw new Exception("The NodeRegistry has already been initialized!");
        }

        singletonInstance = actorSystem.actorOf(Props.create(NodeRegistry.class), NAME);
    }

    public static ActorRef getLocalActor()
    {
        return singletonInstance;
    }

    private NodeRegistry()
    {
        setModel(createModel());
        setControl(createControl(model()));
    }

    private NodeRegistryModel createModel()
    {
        return NodeRegistryModel.builder()
                                .selfProvider(this::self)
                                .senderProvider(this::self)
                                .messageDispatcherProvider(MessageDispatcher::getLocalSingleton)
                                .workDispatcherProvider(WorkDispatcher::getLocalSingleton)
                                .childFactory(context()::actorOf)
                                .watchActorCallback(context()::watch)
                                .build();
    }

    private NodeRegistryControl createControl(NodeRegistryModel model)
    {
        return new NodeRegistryControl(model);
    }

    @Override
    public Receive createReceive()
    {
        return defaultReceiveBuilder().match(NodeRegistryMessages.RegisterWorkerNodeMessage.class, control()::onRegistration)
                                      .build();
    }
}
