package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.calculator;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.Counter;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.store.MatrixStore;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class PCACalculatorModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private final Counter currentCalculationStep = new Counter(0);
    @Getter @Setter
    private Map<Long, RootActorPath> processorIndices;
    @Getter @Setter
    private long myProcessorIndex;
    @Getter @Setter
    private MatrixStore<Double> projection;
    @Builder.Default @Getter @Setter
    private float minimumRecord = Float.MAX_VALUE;
    @Builder.Default @Getter @Setter
    private float maximumRecord = Float.MIN_VALUE;
    @NonNull @Getter
    private final Map<RootActorPath, Long> numberOfRows = new HashMap<>();
    @NonNull @Getter
    private final Map<RootActorPath, MatrixStore<Double>> transposedColumnMeans = new HashMap<>();
    @Getter @Setter
    private MatrixStore<Double> localR;
    @NonNull @Getter
    private final Map<Long, MatrixStore<Double>> remoteRsByProcessStep = new HashMap<>();
}
