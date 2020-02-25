package de.hpi.msc.jschneider.protocol.actorPool.rootActor;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Scheduler;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import scala.concurrent.ExecutionContextExecutor;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Callable;

@SuperBuilder
public class ActorPoolRootActorModel extends AbstractProtocolParticipantModel
{
    @Getter
    private int maximumNumberOfWorkers;
    @NonNull @Getter
    private final Queue<ActorRef> workers = new ArrayDeque<>();
    @NonNull @Getter
    private final Queue<ActorPoolMessages.WorkMessage> workQueue = new ArrayDeque<>();
    @NonNull
    private Callable<Scheduler> schedulerProvider;
    @NonNull
    private Callable<ExecutionContextExecutor> dispatcherProvider;
    @Setter @Getter
    private Cancellable measureUtilizationTask;

    public final Scheduler getScheduler()
    {
        try
        {
            return schedulerProvider.call();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve Scheduler!", exception);
            return null;
        }
    }

    public final ExecutionContextExecutor getDispatcher()
    {
        try
        {
            return dispatcherProvider.call();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve Dispatcher!", exception);
            return null;
        }
    }
}
