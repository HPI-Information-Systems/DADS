package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.protocol.common.BaseProtocol;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherControl;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherModel;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherModel;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor.EqualSequenceSliceDistributorFactory;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor.NullSequenceSliceDistributorFactory;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor.SequenceSliceDistributionRootActorControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor.SequenceSliceDistributionRootActorModel;
import lombok.val;
import lombok.var;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class SequenceSliceDistributionProtocol
{
    public static final boolean IS_ENABLED = true;

    private static final Logger Log = LogManager.getLogger(SequenceSliceDistributionProtocol.class);
    private static final String ROOT_ACTOR_NAME = "SequenceSliceDistributionRootActor";
    private static final String EVENT_DISPATCHER_NAME = "SequenceSliceDistributionEventDispatcher";

    public static void initializeInPlace(Set<Protocol> localProtocols, ActorSystem actorSystem)
    {
        if (IS_ENABLED)
        {
            localProtocols.add(initialize(actorSystem));
        }
    }

    private static Protocol initialize(ActorSystem actorSystem)
    {
        val localProtocol = BaseProtocol.builder()
                                        .type(ProtocolType.SequenceSliceDistribution)
                                        .rootActor(createRootActor(actorSystem))
                                        .eventDispatcher(createEventDispatcher(actorSystem))
                                        .build();

        Log.info("{} successfully initialized.", SequenceSliceDistributionProtocol.class.getName());
        return localProtocol;
    }

    private static ActorRef createRootActor(ActorSystem actorSystem)
    {
        var distributorFactory = NullSequenceSliceDistributorFactory.get();
        if (SystemParameters.getCommand() instanceof MasterCommand)
        {
            val masterCommand = (MasterCommand) SystemParameters.getCommand();
            distributorFactory = EqualSequenceSliceDistributorFactory.fromMasterCommand(masterCommand);
        }

        val model = SequenceSliceDistributionRootActorModel.builder()
                                                           .sliceDistributorFactory(distributorFactory)
                                                           .build();
        val control = new SequenceSliceDistributionRootActorControl(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), ROOT_ACTOR_NAME);
    }

    private static ActorRef createEventDispatcher(ActorSystem actorSystem)
    {
        val model = BaseEventDispatcherModel.create(SequenceSliceDistributionEvents.SubSequenceParametersReceivedEvent.class,
                                                    SequenceSliceDistributionEvents.ProjectionCreatedEvent.class,
                                                    SequenceSliceDistributionEvents.ProjectionCreationCompletedEvent.class);
        val control = new BaseEventDispatcherControl<EventDispatcherModel>(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), EVENT_DISPATCHER_NAME);
    }
}
