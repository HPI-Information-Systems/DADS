package de.hpi.msc.jschneider.protocol.heartbeat;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.protocol.common.BaseProtocol;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherControl;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherModel;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherModel;
import de.hpi.msc.jschneider.protocol.heartbeat.rootActor.HeartbeatRootActorControl;
import de.hpi.msc.jschneider.protocol.heartbeat.rootActor.HeartbeatRootActorModel;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Set;

public class HeartbeatProtocol
{
    public static final boolean IS_ENABLED = false;
    public static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(5L);
    public static final Duration HEARTBEAT_WARNING_INTERVAL = Duration.ofSeconds(10L);
    public static final Duration HEARTBEAT_PANIC_INTERVAL = Duration.ofSeconds(15L);
    public static final Duration HEARTBEAT_CHECK_INTERVAL = Duration.ofSeconds(1L);

    private static final Logger Log = LogManager.getLogger(HeartbeatProtocol.class);
    private static final String ROOT_ACTOR_NAME = "HeartbeatRootActor";
    private static final String EVENT_DISPATCHER_NAME = "HeartbeatEventDispatcher";

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
                                        .type(ProtocolType.Heartbeat)
                                        .rootActor(createRootActor(actorSystem))
                                        .eventDispatcher(createEventDispatcher(actorSystem))
                                        .build();

        Log.info("{} successfully initialized.", HeartbeatProtocol.class.getName());
        return localProtocol;
    }

    private static ActorRef createRootActor(ActorSystem actorSystem)
    {
        val model = HeartbeatRootActorModel.builder()
                                           .dispatcherProvider(actorSystem::dispatcher)
                                           .schedulerProvider(actorSystem::scheduler)
                                           .build();
        val control = new HeartbeatRootActorControl(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), ROOT_ACTOR_NAME);
    }

    private static ActorRef createEventDispatcher(ActorSystem actorSystem)
    {
        val model = BaseEventDispatcherModel.create(HeartbeatEvents.StillAliveEvent.class,
                                                    HeartbeatEvents.HeartbeatPanicEvent.class);
        val control = new BaseEventDispatcherControl<EventDispatcherModel>(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), EVENT_DISPATCHER_NAME);
    }
}
