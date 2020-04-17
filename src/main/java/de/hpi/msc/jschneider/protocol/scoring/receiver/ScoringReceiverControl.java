package de.hpi.msc.jschneider.protocol.scoring.receiver;

import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.fileHandling.FileMerger;
import de.hpi.msc.jschneider.fileHandling.writing.ClearSequenceWriter;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.scoring.ScoringEvents;
import de.hpi.msc.jschneider.protocol.scoring.ScoringMessages;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.FileDoubleSink;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import lombok.SneakyThrows;
import lombok.val;

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class ScoringReceiverControl extends AbstractProtocolParticipantControl<ScoringReceiverModel>
{
    public ScoringReceiverControl(ScoringReceiverModel model)
    {
        super(model);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.ResponsibilitiesReceivedEvent.class);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(NodeCreationEvents.ResponsibilitiesReceivedEvent.class, this::onResponsibilitiesReceived)
                    .match(ScoringMessages.InitializePathScoresTransferMessage.class, this::onInitializePathScoresTransfer);
    }

    private void onResponsibilitiesReceived(NodeCreationEvents.ResponsibilitiesReceivedEvent message)
    {
        try
        {
            assert !getModel().isResponsibilitiesReceived() : "Responsibilities received already!";

            getModel().setResponsibilitiesReceived(true);
            getModel().getRunningDataTransfers().addAll(message.getSubSequenceResponsibilities().keySet());
            getModel().setSubSequenceResponsibilities(new HashMap<>(message.getSubSequenceResponsibilities()));
            getModel().setTemporaryPathScoreFiles(new Object2ObjectLinkedOpenHashMap<>(getModel().getSubSequenceResponsibilities().size()));

            val it = getModel().getSubSequenceResponsibilities().entrySet()
                               .stream()
                               .sorted(Comparator.comparingLong(entry -> entry.getValue().getFrom()))
                               .iterator();
            while (it.hasNext())
            {
                val entry = it.next();
                val path = ((MasterCommand) SystemParameters.getCommand()).getOutputFilePath().toString() + "~" + entry.getValue().getFrom();
                getModel().getTemporaryPathScoreFiles().put(entry.getKey(), new File(path));
            }
        }
        finally
        {
            complete(message);
        }
    }

    @SneakyThrows
    private void onInitializePathScoresTransfer(ScoringMessages.InitializePathScoresTransferMessage message)
    {
        assert getModel().isResponsibilitiesReceived() : "Responsibilities were not received yet!";

        val sender = ProcessorId.of(message.getSender());
        val fileSink = new FileDoubleSink(getModel().getTemporaryPathScoreFiles().get(sender));

        getModel().getDataTransferManager().accept(message, dataReceiver -> dataReceiver.addSink(fileSink)
                                                                                        .whenFinished(receiver -> onPathScoresTransferFinished(sender)));
    }

    private void onPathScoresTransferFinished(ProcessorId workerSystem)
    {
        getModel().getRunningDataTransfers().remove(workerSystem);
        mergeResults();
    }

    @SneakyThrows
    private void mergeResults()
    {
        if (!isReadyToMergeResults())
        {
            return;
        }

        val it = getModel().getSubSequenceResponsibilities().entrySet()
                           .stream()
                           .sorted(Comparator.comparingLong(entry -> entry.getValue().getFrom()))
                           .map(entry -> getModel().getTemporaryPathScoreFiles().get(entry.getKey()))
                           .iterator();

        val resultFile = ((MasterCommand) SystemParameters.getCommand()).getOutputFilePath().toFile();

        val startTime = LocalDateTime.now();
        (new FileMerger(resultFile, it)).merge();
        val endTime = LocalDateTime.now();

        trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.ResultsPersistedEvent.builder()
                                                                                                       .sender(getModel().getSelf())
                                                                                                       .receiver(eventDispatcher)
                                                                                                       .startTime(startTime)
                                                                                                       .endTime(endTime)
                                                                                                       .build());

        getLog().info("================================================================================================");
        getLog().info("================================================================================================");
        getLog().info("Results written to {} in {}.", resultFile.toString(), Duration.between(startTime, endTime));
        getLog().info("================================================================================================");
        getLog().info("================================================================================================");

        trySendEvent(ProtocolType.Scoring, eventDispatcher -> ScoringEvents.ReadyForTerminationEvent.builder()
                                                                                                    .sender(getModel().getSelf())
                                                                                                    .receiver(eventDispatcher)
                                                                                                    .build());
        isReadyToBeTerminated();
    }

    private boolean isReadyToMergeResults()
    {
        return getModel().isResponsibilitiesReceived()
               && getModel().getRunningDataTransfers().isEmpty();
    }
}
