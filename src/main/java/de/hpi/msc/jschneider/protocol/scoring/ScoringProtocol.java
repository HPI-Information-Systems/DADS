package de.hpi.msc.jschneider.protocol.scoring;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.protocol.common.BaseProtocol;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherControl;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherModel;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherModel;
import de.hpi.msc.jschneider.protocol.scoring.rootActor.ScoringRootActorControl;
import de.hpi.msc.jschneider.protocol.scoring.rootActor.ScoringRootActorModel;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ScoringProtocol
{
    private static final Logger Log = LogManager.getLogger(ScoringProtocol.class);
    private static final String ROOT_ACTOR_NAME = "ScoringRootActor";
    private static final String EVENT_DISPATCHER_NAME = "ScoringEventDispatcher";

    public static Protocol initialize(ActorSystem actorSystem)
    {
        val localProtocol = BaseProtocol.builder()
                                        .type(ProtocolType.Scoring)
                                        .rootActor(createRootActor(actorSystem))
                                        .eventDispatcher(createEventDispatcher(actorSystem))
                                        .build();

        Log.info(String.format("%1$s successfully initialized.", ScoringProtocol.class.getName()));
        return localProtocol;
    }

    private static ActorRef createRootActor(ActorSystem actorSystem)
    {
        val model = ScoringRootActorModel.builder()
                                         .build();
        val control = new ScoringRootActorControl(model);

        return actorSystem.actorOf(ProtocolParticipant.props(control), ROOT_ACTOR_NAME);
    }

    private static ActorRef createEventDispatcher(ActorSystem actorSystem)
    {
        val model = BaseEventDispatcherModel.create(); // TODO: add events
        val control = new BaseEventDispatcherControl<EventDispatcherModel>(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), EVENT_DISPATCHER_NAME);
    }
}
