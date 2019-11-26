package de.hpi.msc.jschneider.actor.slave.initializer;

import akka.actor.ActorSelection;
import akka.actor.Address;
import akka.actor.Cancellable;
import akka.actor.Scheduler;
import de.hpi.msc.jschneider.actor.common.AbstractActorModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import scala.concurrent.ExecutionContextExecutor;

import java.util.concurrent.Callable;
import java.util.function.Function;

@SuperBuilder
public class SlaveInitializerModel extends AbstractActorModel
{
    @NonNull @Getter
    private Address nodeRegistryAddress;
    @Getter @Setter
    private Cancellable registrationSchedule;
    @NonNull @Getter
    private Function<String, ActorSelection> actorSelectionProvider;
    @NonNull
    private Callable<Scheduler> schedulerProvider;
    @NonNull
    private Callable<ExecutionContextExecutor> dispatcherProvider;

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
