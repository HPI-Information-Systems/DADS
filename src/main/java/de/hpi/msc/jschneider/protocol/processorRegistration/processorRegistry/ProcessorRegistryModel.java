package de.hpi.msc.jschneider.protocol.processorRegistration.processorRegistry;

import akka.actor.ActorSelection;
import akka.actor.Cancellable;
import akka.actor.Scheduler;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationMessages;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import scala.concurrent.ExecutionContextExecutor;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;

@SuperBuilder
public class ProcessorRegistryModel extends AbstractProtocolParticipantModel
{
    @Getter
    private int expectedNumberOfProcessors;
    @Getter @Setter
    private Cancellable registrationSchedule;
    @NonNull @Getter
    private Function<String, ActorSelection> actorSelectionCallback;
    @Getter @Builder.Default
    private final Map<ProcessorId, ProcessorRegistrationMessages.ProcessorRegistrationMessage> registrationMessages = new HashMap<>();
    @Getter @Setter @Builder.Default
    private boolean localRegistrationReceived = false;
    @NonNull @Getter @Builder.Default
    private Set<ProcessorId> acknowledgedRegistrationMessages = new HashSet<>();
    @NonNull
    private Callable<Scheduler> schedulerProvider;
    @NonNull
    private Callable<ExecutionContextExecutor> dispatcherProvider;
    @Getter @Setter @Builder.Default
    private Duration resendRegistrationInterval = Duration.ofSeconds(5);
    @NonNull @Getter
    private final Map<ProcessorId, Processor> clusterProcessors = new HashMap<>();

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
