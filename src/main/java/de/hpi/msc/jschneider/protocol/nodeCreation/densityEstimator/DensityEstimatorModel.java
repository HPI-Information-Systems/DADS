package de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.calculator.DensityCalculatorMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import it.unimi.dsi.fastutil.doubles.DoubleIterable;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

@SuperBuilder
public class DensityEstimatorModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private ActorRef supervisor;
    @Getter
    private int intersectionSegment;
    @NonNull @Getter
    private Set<ProcessorId> participants;
    @NonNull @Getter
    private DoubleBigList samples;
    @NonNull @Getter
    private double[] pointsToEvaluate;
    @Setter @Getter
    private double whitening;
    @Setter @Getter
    private double normalizationFactor;
    @Setter @Getter
    private double weight;
    @Setter @Getter
    private double[] probabilities;
    @Setter @Getter
    private int expectedNumberOfResults;
    @NonNull @Getter
    private final Map<Integer, double[]> probabilityChunks = new HashMap<>();
    @Setter @Getter
    private Consumer<DensityCalculatorMessages.DensityProbabilitiesEstimatedMessage> resultChunkMerger;
}
