package de.hpi.msc.jschneider.actor.master.nodeRegistry;

import akka.actor.ActorRef;
import lombok.Builder;
import lombok.Getter;

@Builder @Getter
public class WorkerNode
{
    private ActorRef messageDispatcher;
    private ActorRef workDispatcher;
    private int numberOfWorkers;
    private long maximumMemory;

    public static WorkerNode fromRegistrationMessage(NodeRegistryMessages.RegisterWorkerNodeMessage message)
    {
        return builder().messageDispatcher(message.getMessageDispatcher())
                        .workDispatcher(message.getWorkerDispatcher())
                        .numberOfWorkers(message.getNumberOfWorkers())
                        .maximumMemory(message.getMaximumMemory())
                        .build();
    }
}
