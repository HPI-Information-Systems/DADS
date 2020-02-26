package de.hpi.msc.jschneider.protocol.actorPool;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.protocol.actorPool.rootActor.ActorPoolRootActorControl;
import de.hpi.msc.jschneider.protocol.actorPool.rootActor.ActorPoolRootActorModel;
import de.hpi.msc.jschneider.protocol.common.BaseProtocol;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherControl;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherModel;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherModel;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ActorPoolProtocol
{
    private static final Logger Log = LogManager.getLogger(ActorPoolProtocol.class);
    private static final String ROOT_ACTOR_NAME = "ActorPoolRootActor";
    private static final String EVENT_DISPATCHER_NAME = "ActorPoolEventDispatcher";

    public static Protocol initialize(ActorSystem actorSystem)
    {
        val localProtocol = BaseProtocol.builder()
                                        .type(ProtocolType.ActorPool)
                                        .rootActor(createRootActor(actorSystem))
                                        .eventDispatcher(createEventDispatcher(actorSystem))
                                        .build();

        Log.info(String.format("%1$s successfully initialized.", ActorPoolProtocol.class.getName()));
        return localProtocol;
    }

    private static ActorRef createRootActor(ActorSystem actorSystem)
    {
        val model = ActorPoolRootActorModel.builder()
                                           .maximumNumberOfWorkers(SystemParameters.getNumberOfWorkers())
                                           .schedulerProvider(actorSystem::scheduler)
                                           .dispatcherProvider(actorSystem::dispatcher)
                                           .build();
        val control = new ActorPoolRootActorControl(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), ROOT_ACTOR_NAME);
    }

    private static ActorRef createEventDispatcher(ActorSystem actorSystem)
    {
        val model = BaseEventDispatcherModel.create(ActorPoolEvents.UtilizationEvent.class);
        val control = new BaseEventDispatcherControl<EventDispatcherModel>(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), EVENT_DISPATCHER_NAME);
    }
}
