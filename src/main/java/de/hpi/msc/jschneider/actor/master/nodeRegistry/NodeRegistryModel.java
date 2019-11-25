package de.hpi.msc.jschneider.actor.master.nodeRegistry;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.actor.common.AbstractActorModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

@SuperBuilder
public class NodeRegistryModel extends AbstractActorModel
{
    @NonNull
    private Callable<ActorRef> workDispatcherProvider;
    @NonNull @Getter
    private final Map<RootActorPath, WorkerNode> workerNodes = new HashMap<>();

    public final ActorRef getWorkDispatcher()
    {
        try
        {
            return workDispatcherProvider.call();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve WorkDispatcher!", exception);
            return ActorRef.noSender();
        }
    }
}
