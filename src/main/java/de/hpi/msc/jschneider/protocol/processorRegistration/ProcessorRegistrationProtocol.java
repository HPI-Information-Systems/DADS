package de.hpi.msc.jschneider.protocol.processorRegistration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.BaseProtocol;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeProtocol;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeProtocolParticipant;
import de.hpi.msc.jschneider.protocol.processorRegistration.processorRegistry.ProcessorRegistryControl;
import de.hpi.msc.jschneider.protocol.processorRegistration.processorRegistry.ProcessorRegistryModel;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class ProcessorRegistrationProtocol
{
    public static final String ROOT_ACTOR_NAME = "ProcessorRegistrationRootActor";
    public static final String EVENT_DISPATCHER_NAME = "ProcessorRegistrationEventDispatcher";
    private static final Logger Log = LogManager.getLogger(ProcessorRegistrationProtocol.class);
    private static ProcessorRegistryModel rootActorModel;

    private static Protocol localProtocol;

    public static Protocol initialize(ActorSystem actorSystem, ProcessorRole role)
    {
        if (localProtocol != null)
        {
            Log.warn(String.format("%1$s has already been initialized!", ProcessorRegistrationProtocol.class.getName()));
            return localProtocol;
        }

        val localProcessor = BaseProcessor.builder()
                                          .rootPath(RootActorPath.apply(actorSystem.provider().getDefaultAddress(), actorSystem.name()))
                                          .protocols(initializeProtocols(actorSystem, role))
                                          .build();

        localProtocol = BaseProtocol.builder()
                                    .type(ProtocolType.ProcessorRegistration)
                                    .rootActor(createRootActor(actorSystem, localProcessor))
                                    .eventDispatcher(createEventDispatcher(actorSystem))
                                    .build();


        Log.info(String.format("%1$s successfully initialized.", ProcessorRegistrationProtocol.class.getName()));
        return localProtocol;
    }

    private static Set<Protocol> initializeProtocols(ActorSystem actorSystem, ProcessorRole role)
    {
        val protocols = new HashSet<Protocol>();

        switch (role)
        {
            case Worker:
            {
                protocols.addAll(initializeWorkerProtocols(actorSystem));
                break;
            }
            default:
            {
                Log.error(String.format("Unknown %1$s!", ProcessorRole.class.getName()));
                break;
            }
        }

        return protocols;
    }

    private static Set<Protocol> initializeWorkerProtocols(ActorSystem actorSystem)
    {
        val protocols = new HashSet<Protocol>();

        protocols.add(MessageExchangeProtocol.initialize(actorSystem));

        return protocols;
    }

    private static ActorRef createRootActor(ActorSystem actorSystem, Processor localProcessor)
    {
        rootActorModel = ProcessorRegistryModel.builder()
                                               .localProcessor(localProcessor)
                                               .actorSelectionCallback(actorSystem::actorSelection)
                                               .schedulerProvider(actorSystem::scheduler)
                                               .dispatcherProvider(actorSystem::dispatcher)
                                               .build();
        val control = new ProcessorRegistryControl(rootActorModel);

        return actorSystem.actorOf(MessageExchangeProtocolParticipant.props(control), ROOT_ACTOR_NAME);
    }

    private static ActorRef createEventDispatcher(ActorSystem actorSystem)
    {
        // TODO: add event dispatcher model and control
        return ActorRef.noSender();
    }

    public static ActorRef getLocalRootActor()
    {
        return localProtocol.getRootActor();
    }

    public static ActorRef getLocalEventDispatcher()
    {
        return localProtocol.getEventDispatcher();
    }
}
