package de.hpi.msc.jschneider.protocol.dimensionReduction;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.protocol.common.BaseProtocol;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherControl;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherModel;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherModel;
import de.hpi.msc.jschneider.protocol.dimensionReduction.rootActor.DimensionReductionRootActorControl;
import de.hpi.msc.jschneider.protocol.dimensionReduction.rootActor.DimensionReductionRootActorModel;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class DimensionReductionProtocol
{
    public static final boolean IS_ENABLED = true;

    private static final Logger Log = LogManager.getLogger(DimensionReductionProtocol.class);
    private static final String ROOT_ACTOR_NAME = "DimensionReductionRootActor";
    private static final String EVENT_DISPATCHER_NAME = "DimensionReductionEventDispatcher";

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
                                        .type(ProtocolType.DimensionReduction)
                                        .rootActor(createRootActor(actorSystem))
                                        .eventDispatcher(createEventDispatcher(actorSystem))
                                        .build();

        Log.info("{} successfully initialized.", DimensionReductionProtocol.class.getName());
        return localProtocol;
    }

    private static ActorRef createRootActor(ActorSystem actorSystem)
    {
        val model = DimensionReductionRootActorModel.builder().build();
        val control = new DimensionReductionRootActorControl(model);

        return actorSystem.actorOf(ProtocolParticipant.props(control), ROOT_ACTOR_NAME);
    }

    private static ActorRef createEventDispatcher(ActorSystem actorSystem)
    {
        val model = BaseEventDispatcherModel.create(DimensionReductionEvents.ReducedProjectionCreatedEvent.class);
        val control = new BaseEventDispatcherControl<EventDispatcherModel>(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), EVENT_DISPATCHER_NAME);
    }
}
