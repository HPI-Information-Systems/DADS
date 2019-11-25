package de.hpi.msc.jschneider.actor.master.nodeRegistry;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.actor.common.AbstractMessage;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class NodeRegistryMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class RegisterWorkerNodeMessage extends AbstractMessage
    {
        private static final long serialVersionUID = -6601179746857883050L;
        private ActorRef messageDispatcher;
        private ActorRef workDispatcher;
        private int numberOfWorkers;
        private long maximumMemory;
    }
}
