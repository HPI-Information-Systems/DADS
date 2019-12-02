package de.hpi.msc.jschneider.protocol.messageExchange;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.protocol.common.BaseProtocol;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherControl;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.BaseEventDispatcherModel;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherModel;
import de.hpi.msc.jschneider.protocol.messageExchange.messageDispatcher.MessageDispatcherControl;
import de.hpi.msc.jschneider.protocol.messageExchange.messageDispatcher.MessageDispatcherModel;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MessageExchangeProtocol
{
    private static final Logger Log = LogManager.getLogger(MessageExchangeProtocol.class);
    private static final String ROOT_ACTOR_NAME = "MessageExchangeRootActor";
    private static final String EVENT_DISPATCHER_NAME = "MessageExchangeEventDispatcher";

    private static Protocol localProtocol;

    public static Protocol initialize(ActorSystem actorSystem)
    {
        if (localProtocol != null)
        {
            Log.warn(String.format("%1$s has already been initialized!", MessageExchangeProtocol.class.getName()));
            return localProtocol;
        }

        localProtocol = BaseProtocol.builder()
                                    .type(ProtocolType.MessageExchange)
                                    .rootActor(createRootActor(actorSystem))
                                    .eventDispatcher(createEventDispatcher(actorSystem))
                                    .build();

        Log.info(String.format("%1$s successfully initialized.", MessageExchangeProtocol.class.getName()));
        return localProtocol;
    }

    private static ActorRef createRootActor(ActorSystem actorSystem)
    {
        val model = MessageDispatcherModel.builder().build();
        val control = new MessageDispatcherControl(model);

        return actorSystem.actorOf(ProtocolParticipant.props(control), ROOT_ACTOR_NAME);
    }

    private static ActorRef createEventDispatcher(ActorSystem actorSystem)
    {
        val model = BaseEventDispatcherModel.create(); // TODO: add event message classes
        val control = new BaseEventDispatcherControl<EventDispatcherModel>(model);

        return actorSystem.actorOf(ProtocolParticipant.props(control), EVENT_DISPATCHER_NAME);
    }

    public static boolean isInitialized()
    {
        return localProtocol != null;
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
