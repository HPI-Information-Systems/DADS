package de.hpi.msc.jschneider.actor.slave.initializer;

import akka.actor.Address;
import akka.actor.Props;
import de.hpi.msc.jschneider.actor.common.AbstractActor;
import de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher.MessageDispatcher;

public class SlaveInitializer extends AbstractActor<SlaveInitializerModel, SlaveInitializerControl>
{
    private SlaveInitializer(Address nodeRegistryAddress)
    {
        setModel(createModel(nodeRegistryAddress));
        setControl(createControl(model()));
    }

    public static Props props(Address nodeRegistryAddress)
    {
        return Props.create(SlaveInitializer.class, () -> new SlaveInitializer(nodeRegistryAddress));
    }

    private SlaveInitializerModel createModel(Address nodeRegistryAddress)
    {
        return SlaveInitializerModel.builder()
                                    .selfProvider(this::self)
                                    .senderProvider(this::sender)
                                    .messageDispatcherProvider(MessageDispatcher::getLocalSingleton)
                                    .childFactory(context()::actorOf)
                                    .watchActorCallback(context()::watch)
                                    .nodeRegistryAddress(nodeRegistryAddress)
                                    .actorSelectionProvider(context().system()::actorSelection)
                                    .schedulerProvider(context().system()::scheduler)
                                    .dispatcherProvider(context().system()::dispatcher)
                                    .build();
    }

    private SlaveInitializerControl createControl(SlaveInitializerModel model)
    {
        return new SlaveInitializerControl(model);
    }

    @Override
    public Receive createReceive()
    {
        return defaultReceiveBuilder().build();
    }
}
