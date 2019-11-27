package de.hpi.msc.jschneider.protocol.reaper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.protocol.common.BaseProtocol;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReaperProtocol
{
    private static final Logger Log = LogManager.getLogger(ReaperProtocol.class);
    private static final String ROOT_ACTOR_NAME = "ReaperRootActor";
    private static final String EVENT_DISPATCHER_NAME = "ReaperEventDispatcher";

    private static Protocol localProtocol;

    public static Protocol initialize(ActorSystem actorSystem)
    {
        if (localProtocol != null)
        {
            Log.warn(String.format("%1$s has already been initialized!", ReaperProtocol.class.getName()));
            return localProtocol;
        }

        localProtocol = BaseProtocol.builder()
                                    .type(ProtocolType.Reaper)
                                    .rootActor(createRootActor(actorSystem))
                                    .eventDispatcher(createEventDispatcher(actorSystem))
                                    .build();

        Log.info(String.format("%1$s successfully initialized.", ReaperProtocol.class.getName()));
        return localProtocol;
    }

    private static ActorRef createRootActor(ActorSystem actorSystem)
    {
        // TODO:
        return ActorRef.noSender();
    }

    private static ActorRef createEventDispatcher(ActorSystem actorSystem)
    {
        // TODO: add event dispatcher model and control
        return ActorRef.noSender();
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
