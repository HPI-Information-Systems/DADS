package de.hpi.msc.jschneider.protocol.statistics.rootActor;

import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationEvents;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.protocol.scoring.ScoringEvents;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import lombok.val;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StatisticsLog
{
    public static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS");

    private FileOutputStream outputStream;
    private Writer outputWriter;
    private boolean isOpen = true;

    public StatisticsLog(File file)
    {
        try
        {
            outputStream = createOpenOutputStream(file);
            outputWriter = new PrintWriter(outputStream);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            isOpen = false;
            outputWriter = null;
        }

    }

    private FileOutputStream createOpenOutputStream(File file) throws Exception
    {
        if (file == null)
        {
            throw new NullPointerException("File must not be null!");
        }

        file.delete();

        if (!file.exists())
        {
            if (!file.getParentFile().exists() && !file.getParentFile().mkdirs())
            {
                throw new Exception(String.format("Unable to create directory of %1$s!", file.getAbsolutePath()));
            }

            if (!file.createNewFile())
            {
                throw new Exception(String.format("Unable to create file %1$s!", file.getAbsolutePath()));
            }
        }

        if (!file.isFile())
        {
            throw new IllegalArgumentException(String.format("%1$s is not a file!", file.getAbsolutePath()));
        }

        if (!file.canWrite())
        {
            throw new IllegalArgumentException(String.format("Unable to write to %1$s!", file.getAbsolutePath()));
        }

        return new FileOutputStream(file.getAbsolutePath(), false);
    }

    public void log(StatisticsRootActorControl control, ProcessorRegistrationEvents.RegistrationAcknowledgedEvent event)
    {
        tryWrite(String.format("RegistrationAcknowledged { Processor = %1$s; StartTime = %2$s; }",
                               ProcessorId.of(control.getModel().getSelf()),
                               control.getModel().getCalculationStartTime().format(DATE_FORMAT)));
    }

    public void log(StatisticsRootActorControl control, StatisticsEvents.DataTransferCompletedEvent event)
    {
        tryWrite(String.format("DataTransferCompleted { Processor = %1$s; Type = %2$s; Source = %3$s; Sink = %4$s; StartTime = %5$s; EndTime = %6$s; Bytes = %7$d; }",
                               event.getProcessor(),
                               event.getInitializationMessageType().getSimpleName(),
                               event.getSource(),
                               event.getSink(),
                               event.getStartTime().format(DATE_FORMAT),
                               event.getEndTime().format(DATE_FORMAT),
                               event.getTransferredBytes()));
    }

    public void log(StatisticsRootActorControl control, NodeCreationEvents.NodePartitionCreationCompletedEvent event)
    {
        val duration = Duration.between(event.getStartTime(), event.getEndTime());

        tryWrite(String.format("NodePartitionCreationCompleted { StartTime = %1$s; EndTime = %2$s; Duration = %3$s; }",
                               event.getStartTime().format(DATE_FORMAT),
                               event.getEndTime().format(DATE_FORMAT),
                               duration));
    }

    public void log(StatisticsRootActorControl control, NodeCreationEvents.NodeCreationCompletedEvent event)
    {
        val duration = Duration.between(event.getStartTime(), event.getEndTime());

        tryWrite(String.format("NodeCreationCompleted { StartTime = %1$s; EndTime = %2$s; Duration = %3$s; }",
                               event.getStartTime().format(DATE_FORMAT),
                               event.getEndTime().format(DATE_FORMAT),
                               duration));
    }

    public void log(StatisticsRootActorControl control, EdgeCreationEvents.EdgePartitionCreationCompletedEvent event)
    {
        val duration = Duration.between(event.getStartTime(), event.getEndTime());

        tryWrite(String.format("EdgePartitionCreationCompleted { StartTime = %1$s; EndTime = %2$s; Duration = %3$s; }",
                               event.getStartTime().format(DATE_FORMAT),
                               event.getEndTime().format(DATE_FORMAT),
                               duration));
    }

    public void log(StatisticsRootActorControl control, PCAEvents.PrincipalComponentComputationCompletedEvent event)
    {
        val duration = Duration.between(event.getStartTime(), event.getEndTime());

        tryWrite(String.format("PrincipalComponentComputationCompleted { StartTime = %1$s; EndTime = %2$s; Duration = %3$s; }",
                               event.getStartTime().format(DATE_FORMAT),
                               event.getEndTime().format(DATE_FORMAT),
                               duration));
    }

    public void log(StatisticsRootActorControl control, StatisticsEvents.UtilizationEvent event)
    {
        tryWrite(String.format("Utilization { Processor = %1$s; DateTime = %2$s; MaximumMemory = %3$d; FreeMemory = %4$d; UsedMemory = %5$d; CPULoad = %6$f; }",
                               ProcessorId.of(event.getSender()),
                               event.getDateTime().format(DATE_FORMAT),
                               event.getMaximumMemoryInBytes(),
                               event.getMaximumMemoryInBytes() - event.getUsedMemoryInBytes(),
                               event.getUsedMemoryInBytes(),
                               event.getCpuUtilization()));
    }

    public void log(StatisticsRootActorControl control, MessageExchangeEvents.UtilizationEvent event)
    {
        tryWrite(String.format("MessageExchangeUtilization { Processor = %1$s; DateTime = %2$s; RemoteProcessor = %3$s; TotalNumberOfEnqueuedMessages = %4$d; TotalNumberOfUnacknowledgedMessages = %5$d; LargestMessageQueueSize = %6$d; LargestMessageQueueReceiver = %7$s; AverageNumberOfEnqueuedMessages = %8$f; }",
                               ProcessorId.of(event.getSender()),
                               event.getDateTime().format(DATE_FORMAT),
                               event.getRemoteProcessor(),
                               event.getTotalNumberOfEnqueuedMessages(),
                               event.getTotalNumberOfUnacknowledgedMessages(),
                               event.getLargestMessageQueueSize(),
                               event.getLargestMessageQueueReceiver(),
                               event.getAverageMessageQueueSize()));
    }

    public void log(StatisticsRootActorControl control, ScoringEvents.ReadyForTerminationEvent event)
    {
        val duration = Duration.between(control.getModel().getCalculationStartTime(), control.getModel().getCalculationEndTime());

        tryWrite(String.format("CalculationCompleted { Processor = %1$s; StartTime = %2$s; EndTime = %3$s; Duration = %4$s; }",
                               ProcessorId.of(control.getModel().getSelf()),
                               control.getModel().getCalculationStartTime().format(DATE_FORMAT),
                               control.getModel().getCalculationEndTime().format(DATE_FORMAT),
                               duration));
    }

    private void tryWrite(String message)
    {
        if (!isOpen)
        {
            return;
        }

        try
        {
            val currentTime = LocalDateTime.now().format(DATE_FORMAT);
            val stringBuilder = new StringBuilder().append("[")
                                                   .append(currentTime)
                                                   .append("]  -  ")
                                                   .append(message)
                                                   .append("\n");

            outputWriter.write(stringBuilder.toString());
            outputWriter.flush();
        }
        catch (IOException ioException)
        {
            ioException.printStackTrace();
            close();
        }
    }

    public void close()
    {
        tryClose();
    }

    private void tryClose()
    {
        if (!isOpen)
        {
            return;
        }

        isOpen = false;

        try
        {
            outputStream.close();
        }
        catch (IOException ioException)
        {
            ioException.printStackTrace();
        }
    }
}
