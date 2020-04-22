package de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.calculator.DensityCalculatorMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

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
    private DoubleBigList pointsToEvaluate;
    @Setter @Getter
    private double whitening;
    @Setter @Getter
    private double normalizationFactor;
    @Setter @Getter
    private double weight;
    @Setter @Getter
    private DoubleBigList probabilities;
    @Setter @Getter
    private long expectedNumberOfResults;
    @Setter @Getter
    private Long2ObjectMap<DoubleBigList> probabilityChunks;
    @Setter @Getter
    private Consumer<DensityCalculatorMessages.DensityProbabilitiesEstimatedMessage> resultChunkMerger;
}
