package de.hpi.msc.jschneider.protocol.principalComponentAnalysis;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.protocol.common.BaseProtocol;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherControl;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherModel;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherModel;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.rootActor.PCARootActorControl;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.rootActor.PCARootActorModel;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PCAProtocol
{
    private static final Logger Log = LogManager.getLogger(PCAProtocol.class);
    private static final String ROOT_ACTOR_NAME = "PrincipalComponentAnalysisRootActor";
    private static final String EVENT_DISPATCHER_NAME = "PrincipalComponentAnalysisEventDispatcher";

    public static Protocol initialize(ActorSystem actorSystem)
    {
        val localProtocol = BaseProtocol.builder()
                                        .type(ProtocolType.PrincipalComponentAnalysis)
                                        .rootActor(createRootActor(actorSystem))
                                        .eventDispatcher(createEventDispatcher(actorSystem))
                                        .build();

        Log.info(String.format("%1$s successfully initialized.", PCAProtocol.class.getName()));
        return localProtocol;
    }

    private static ActorRef createRootActor(ActorSystem actorSystem)
    {
        val model = PCARootActorModel.builder().build();
        val control = new PCARootActorControl(model);

        return actorSystem.actorOf(ProtocolParticipant.props(control), ROOT_ACTOR_NAME);
    }

    private static ActorRef createEventDispatcher(ActorSystem actorSystem)
    {
        val model = BaseEventDispatcherModel.create(PCAEvents.PrincipalComponentsCreatedEvent.class,
                                                    PCAEvents.PrincipalComponentComputationCompletedEvent.class);
        val control = new BaseEventDispatcherControl<EventDispatcherModel>(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), EVENT_DISPATCHER_NAME);
    }
}
