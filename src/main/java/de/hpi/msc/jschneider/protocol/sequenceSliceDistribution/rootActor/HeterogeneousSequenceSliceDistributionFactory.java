package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor;

import akka.actor.Props;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.fileHandling.reading.BinarySequenceReader;
import de.hpi.msc.jschneider.fileHandling.reading.SequenceReader;
import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.protocol.reaper.ReapedActor;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor.SequenceSliceDistributorControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor.SequenceSliceDistributorModel;
import lombok.val;
import lombok.var;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

public class HeterogeneousSequenceSliceDistributionFactory implements SequenceSliceDistributorFactory
{
    public static SequenceSliceDistributorFactory fromMasterCommand(MasterCommand masterCommand)
    {
        return new HeterogeneousSequenceSliceDistributionFactory(masterCommand);
    }

    private final int expectedNumberOfProcessors;
    private final SequenceReader sequenceReaderTemplate;
    private final Set<Processor> processors;
    private final int subSequenceLength;
    private final int sliceOverlap;
    private final int convolutionSize;

    private HeterogeneousSequenceSliceDistributionFactory(MasterCommand masterCommand)
    {
        expectedNumberOfProcessors = masterCommand.getMinimumNumberOfSlaves() + 1; // the master is also working
        processors = new HashSet<>(expectedNumberOfProcessors);
        sequenceReaderTemplate = BinarySequenceReader.fromFile(masterCommand.getSequenceFilePath().toFile());
        subSequenceLength = masterCommand.getSubSequenceLength();
        sliceOverlap = masterCommand.getSubSequenceLength() - 1;
        convolutionSize = masterCommand.getConvolutionSize();
    }

    @Override
    public Collection<ProtocolParticipantControl<? extends ProtocolParticipantModel>> createDistributorsFromNewProcessor(Processor newProcessor)
    {
        processors.add(newProcessor);
        if (processors.size() != expectedNumberOfProcessors)
        {
            return new ArrayList<>(0);
        }

        return createSliceDistributorProps();
    }

    private Collection<ProtocolParticipantControl<? extends ProtocolParticipantModel>> createSliceDistributorProps()
    {
        val controls = new ArrayList<ProtocolParticipantControl<? extends ProtocolParticipantModel>>(processors.size());

        val totalMeasurements = sequenceReaderTemplate.getSize();
        val totalClusterMemory = processors.stream().mapToLong(Processor::getMaximumMemoryInBytes).sum();
        val sortedProcessors = processors.stream().sorted(Comparator.comparingLong(Processor::getMaximumMemoryInBytes)).iterator();

        var nextSubReaderStartPosition = 0L;
        var nextSubSequenceIndex = 0L;

        while (sortedProcessors.hasNext())
        {
            val processor = sortedProcessors.next();
            val memoryShare = processor.getMaximumMemoryInBytes() / (double) totalClusterMemory;
            var sliceLength = (long) Math.ceil(totalMeasurements * memoryShare) + sliceOverlap;
            if (!sortedProcessors.hasNext())
            {
                sliceLength = totalMeasurements - nextSubReaderStartPosition;
            }

            val reader = sequenceReaderTemplate.subReader(nextSubReaderStartPosition, sliceLength);

            val model = SequenceSliceDistributorModel.builder()
                                                     .sliceReceiverActorSystem(processor.getId())
                                                     .sequenceReader(reader)
                                                     .maximumMessageSizeProvider(SystemParameters::getMaximumMessageSize)
                                                     .firstSubSequenceIndex(nextSubSequenceIndex)
                                                     .isLastSubSequenceChunk(!sortedProcessors.hasNext())
                                                     .subSequenceLength(subSequenceLength)
                                                     .convolutionSize(convolutionSize)
                                                     .build();

            val control = new SequenceSliceDistributorControl(model);
            controls.add(control);

            nextSubReaderStartPosition += Math.max(1L, reader.getSize() - sliceOverlap);
            nextSubSequenceIndex += Math.max(1L, reader.getSize() - (subSequenceLength - 1L));
        }

        return controls;
    }
}
