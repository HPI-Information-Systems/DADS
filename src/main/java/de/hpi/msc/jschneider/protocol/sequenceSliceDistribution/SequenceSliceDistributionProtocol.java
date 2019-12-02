package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.protocol.common.BaseProtocol;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherControl;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherModel;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherModel;
import de.hpi.msc.jschneider.protocol.reaper.ReaperEvents;
import de.hpi.msc.jschneider.protocol.reaper.reaper.ReaperControl;
import de.hpi.msc.jschneider.protocol.reaper.reaper.ReaperModel;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SequenceSliceDistributionProtocol
{
    private static final Logger Log = LogManager.getLogger(SequenceSliceDistributionProtocol.class);
    private static final String ROOT_ACTOR_NAME = "SequenceSliceDistributionRootActor";
    private static final String EVENT_DISPATCHER_NAME = "SequenceSliceDistributionEventDispatcher";

    private static Protocol localProtocol;

    public static Protocol initialize(ActorSystem actorSystem)
    {
        if (localProtocol != null)
        {
            Log.warn(String.format("%1$s has already been initialized!", SequenceSliceDistributionProtocol.class.getName()));
            return localProtocol;
        }

        localProtocol = BaseProtocol.builder()
                                    .type(ProtocolType.SequenceSliceDistribution)
                                    .rootActor(createRootActor(actorSystem))
                                    .eventDispatcher(createEventDispatcher(actorSystem))
                                    .build();

        Log.info(String.format("%1$s successfully initialized.", SequenceSliceDistributionProtocol.class.getName()));
        return localProtocol;
    }

    private static ActorRef createRootActor(ActorSystem actorSystem)
    {
        val model = ReaperModel.builder()
                               .terminateActorSystemCallback(actorSystem::terminate)
                               .build();
        val control = new ReaperControl(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), ROOT_ACTOR_NAME);
    }

    private static ActorRef createEventDispatcher(ActorSystem actorSystem)
    {
        val model = BaseEventDispatcherModel.create(ReaperEvents.ActorSystemReapedEvents.class);
        val control = new BaseEventDispatcherControl<EventDispatcherModel>(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), EVENT_DISPATCHER_NAME);
    }
}
