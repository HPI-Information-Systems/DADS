package de.hpi.msc.jschneider.protocol.nodeCreation;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.protocol.common.BaseProtocol;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherControl;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherModel;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherModel;
import de.hpi.msc.jschneider.protocol.nodeCreation.rootActor.NodeCreationRootActorControl;
import de.hpi.msc.jschneider.protocol.nodeCreation.rootActor.NodeCreationRootActorModel;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class NodeCreationProtocol
{
    public static final boolean IS_ENABLED = true;

    private static final Logger Log = LogManager.getLogger(NodeCreationProtocol.class);
    private static final String ROOT_ACTOR_NAME = "NodeCreationRootActor";
    private static final String EVENT_DISPATCHER_NAME = "NodeCreationEventDispatcher";

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
                                        .type(ProtocolType.NodeCreation)
                                        .rootActor(createRootActor(actorSystem))
                                        .eventDispatcher(createEventDispatcher(actorSystem))
                                        .build();

        Log.info("{} successfully initialized.", NodeCreationProtocol.class.getName());
        return localProtocol;
    }

    private static ActorRef createRootActor(ActorSystem actorSystem)
    {
        val model = NodeCreationRootActorModel.builder().build();
        val control = new NodeCreationRootActorControl(model);

        return actorSystem.actorOf(ProtocolParticipant.props(control), ROOT_ACTOR_NAME);
    }

    private static ActorRef createEventDispatcher(ActorSystem actorSystem)
    {
        val model = BaseEventDispatcherModel.create(NodeCreationEvents.ResponsibilitiesReceivedEvent.class,
                                                    NodeCreationEvents.IntersectionsCalculatedEvent.class,
                                                    NodeCreationEvents.NodePartitionCreationCompletedEvent.class,
                                                    NodeCreationEvents.NodeCreationCompletedEvent.class);
        val control = new BaseEventDispatcherControl<EventDispatcherModel>(model);
        return actorSystem.actorOf(ProtocolParticipant.props(control), EVENT_DISPATCHER_NAME);
    }
}
