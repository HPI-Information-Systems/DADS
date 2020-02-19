package de.hpi.msc.jschneider.protocol.statistics.rootActor;

import akka.actor.Cancellable;
import akka.actor.Scheduler;
import com.sun.management.OperatingSystemMXBean;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import scala.concurrent.ExecutionContextExecutor;

import java.lang.management.MemoryMXBean;
import java.time.LocalDateTime;
import java.util.concurrent.Callable;

@SuperBuilder
public class StatisticsRootActorModel extends AbstractProtocolParticipantModel
{
    @Getter
    private StatisticsLog statisticsLog;
    @Setter @Getter
    private LocalDateTime calculationStartTime;
    @Setter @Getter
    private LocalDateTime calculationEndTime;
    @NonNull
    private Callable<Scheduler> schedulerProvider;
    @NonNull
    private Callable<ExecutionContextExecutor> dispatcherProvider;
    @Setter @Getter
    private Cancellable utilizationMeasurementTask;
    @Setter @Getter
    private OperatingSystemMXBean osBean;
    @Setter @Getter
    private MemoryMXBean memoryBean;

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
