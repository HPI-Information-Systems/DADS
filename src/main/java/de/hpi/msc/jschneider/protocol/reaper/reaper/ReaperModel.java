package de.hpi.msc.jschneider.protocol.reaper.reaper;

import akka.actor.Cancellable;
import akka.actor.Scheduler;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import scala.concurrent.ExecutionContextExecutor;

import java.util.concurrent.Callable;

@SuperBuilder
public class ReaperModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter @Setter
    private Runnable terminateActorSystemCallback;
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
