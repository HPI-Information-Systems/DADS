package de.hpi.msc.jschneider.protocol.processorRegistration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.protocol.common.BaseProtocol;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherControl;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherModel;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherModel;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionProtocol;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationProtocol;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingProtocol;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeProtocol;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationProtocol;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAProtocol;
import de.hpi.msc.jschneider.protocol.processorRegistration.processorRegistry.ProcessorRegistryControl;
import de.hpi.msc.jschneider.protocol.processorRegistration.processorRegistry.ProcessorRegistryModel;
import de.hpi.msc.jschneider.protocol.reaper.ReaperProtocol;
import de.hpi.msc.jschneider.protocol.scoring.ScoringProtocol;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionProtocol;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsProtocol;
import lombok.val;
import lombok.var;
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

    public static Protocol initialize(ActorSystem actorSystem, ProcessorRole role, boolean isMaster)
    {
        val localProcessor = BaseProcessor.builder()
                                          .isMaster(isMaster)
                                          .id(ProcessorId.of(actorSystem))
                                          .protocols(initializeProtocols(actorSystem, role))
                                          .build();

        val localProtocol = BaseProtocol.builder()
                                        .type(ProtocolType.ProcessorRegistration)
                                        .rootActor(createRootActor(actorSystem, localProcessor))
                                        .eventDispatcher(createEventDispatcher(actorSystem))
                                        .build();

        val actualProtocols = new Protocol[localProcessor.getProtocols().length + 1];
        System.arraycopy(localProcessor.getProtocols(), 0, actualProtocols, 0, localProcessor.getProtocols().length);
        actualProtocols[actualProtocols.length - 1] = localProtocol;
        localProcessor.setProtocols(actualProtocols);

        setUpProtocols(localProcessor);

        Log.info(String.format("%1$s successfully initialized.", ProcessorRegistrationProtocol.class.getName()));
        return localProtocol;
    }

    private static Protocol[] initializeProtocols(ActorSystem actorSystem, ProcessorRole role)
    {
        val protocols = new HashSet<Protocol>();
        protocols.add(ReaperProtocol.initialize(actorSystem));

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

        return protocols.toArray(new Protocol[0]);
    }

    private static Set<Protocol> initializeWorkerProtocols(ActorSystem actorSystem)
    {
        val protocols = new HashSet<Protocol>();

        protocols.add(MessageExchangeProtocol.initialize(actorSystem));
        protocols.add(SequenceSliceDistributionProtocol.initialize(actorSystem));
        protocols.add(PCAProtocol.initialize(actorSystem));
        protocols.add(DimensionReductionProtocol.initialize(actorSystem));
        protocols.add(NodeCreationProtocol.initialize(actorSystem));
        protocols.add(EdgeCreationProtocol.initialize(actorSystem));
        protocols.add(GraphMergingProtocol.initialize(actorSystem));
        protocols.add(ScoringProtocol.initialize(actorSystem));
        protocols.add(StatisticsProtocol.initialize(actorSystem));

        return protocols;
    }

    private static ActorRef createRootActor(ActorSystem actorSystem, Processor localProcessor)
    {
        var expectedNumberOfProcessors = 0;
        if (SystemParameters.getCommand() instanceof MasterCommand)
        {
            expectedNumberOfProcessors = ((MasterCommand) SystemParameters.getCommand()).getMinimumNumberOfSlaves() + 1; // add one, because the master is always working
        }

        rootActorModel = ProcessorRegistryModel.builder()
                                               .expectedNumberOfProcessors(expectedNumberOfProcessors)
                                               .actorSelectionCallback(actorSystem::actorSelection)
                                               .schedulerProvider(actorSystem::scheduler)
                                               .dispatcherProvider(actorSystem::dispatcher)
                                               .build();
        rootActorModel.getClusterProcessors().put(localProcessor.getId(), localProcessor);
        val control = new ProcessorRegistryControl(rootActorModel);

        return actorSystem.actorOf(ProtocolParticipant.props(control), ROOT_ACTOR_NAME);
    }

    private static ActorRef createEventDispatcher(ActorSystem actorSystem)
    {
        val model = BaseEventDispatcherModel.create(ProcessorRegistrationEvents.ProcessorJoinedEvent.class,
                                                    ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class);
        val control = new BaseEventDispatcherControl<EventDispatcherModel>(model);

        return actorSystem.actorOf(ProtocolParticipant.props(control), EVENT_DISPATCHER_NAME);
    }

    private static void setUpProtocols(Processor localProcessor)
    {
        val message = CommonMessages.SetUpProtocolMessage.builder()
                                                         .localProcessor(localProcessor)
                                                         .build();

        for (val protocol : localProcessor.getProtocols())
        {
            protocol.getRootActor().tell(message, ActorRef.noSender());
        }

        try
        {
            Thread.sleep(1000); // await any event subscriptions
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    public static Processor[] getProcessors()
    {
        return rootActorModel.getClusterProcessors().values().toArray(new Processor[0]);
    }
}
