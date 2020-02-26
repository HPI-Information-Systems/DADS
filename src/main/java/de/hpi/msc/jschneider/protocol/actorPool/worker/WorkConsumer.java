package de.hpi.msc.jschneider.protocol.actorPool.worker;

import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;

@FunctionalInterface
public interface WorkConsumer
{
    void process(ActorPoolWorkerControl control, ActorPoolMessages.WorkMessage workLoad);
}
