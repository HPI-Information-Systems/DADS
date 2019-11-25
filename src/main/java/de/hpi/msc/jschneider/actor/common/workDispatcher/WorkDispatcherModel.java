package de.hpi.msc.jschneider.actor.common.workDispatcher;

import akka.actor.ActorSelection;
import akka.actor.Cancellable;
import akka.actor.Scheduler;
import akka.dispatch.Dispatcher;
import de.hpi.msc.jschneider.actor.common.AbstractActorModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.concurrent.Callable;
import java.util.function.Function;

@SuperBuilder
public class WorkDispatcherModel extends AbstractActorModel
{
    @Getter @Setter
    private Cancellable registerSchedule;
    @NonNull @Getter
    private Function<String, ActorSelection> actorSelectionProvider;
    @NonNull
    private Callable<Scheduler> schedulerProvider;
    @NonNull
    private Callable<Dispatcher> dispatcherProvider;

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

    public final Dispatcher getDispatcher()
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
