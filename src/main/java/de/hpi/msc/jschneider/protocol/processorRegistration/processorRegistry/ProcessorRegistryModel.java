package de.hpi.msc.jschneider.protocol.processorRegistration.processorRegistry;

import akka.actor.ActorSelection;
import akka.actor.Cancellable;
import akka.actor.RootActorPath;
import akka.actor.Scheduler;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import scala.concurrent.ExecutionContextExecutor;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

@SuperBuilder
public class ProcessorRegistryModel extends AbstractProtocolParticipantModel
{
    @Getter @Setter
    private Cancellable registrationSchedule;
    @NonNull @Getter
    private Function<String, ActorSelection> actorSelectionCallback;
    @NonNull
    private Callable<Scheduler> schedulerProvider;
    @NonNull
    private Callable<ExecutionContextExecutor> dispatcherProvider;
    @Getter @Setter @Builder.Default
    private Duration resendRegistrationInterval = Duration.ofSeconds(5);
    @NonNull @Getter
    private final Map<RootActorPath, Processor> processors = new HashMap<>();

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
