package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.calculator;

import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.Counter;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.store.MatrixStore;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class PCACalculatorModel extends AbstractProtocolParticipantModel
{
    @Getter
    private int convolutionSize;
    @NonNull @Getter
    private final Counter currentCalculationStep = new Counter(0);
    @Getter @Setter
    private Map<Long, ProcessorId> processorIndices;
    @Getter @Setter
    private long myProcessorIndex;
    @Getter @Setter
    private MatrixStore<Double> projection;
    @Builder.Default @Getter @Setter
    private double minimumRecord = Double.MAX_VALUE;
    @Builder.Default @Getter @Setter
    private double maximumRecord = Double.MIN_VALUE;
    @NonNull @Getter
    private final Map<ProcessorId, Long> numberOfRows = new HashMap<>();
    @NonNull @Getter
    private final Map<ProcessorId, MatrixStore<Double>> transposedColumnMeans = new HashMap<>();
    @Getter @Setter
    private MatrixStore<Double> localR;
    @NonNull @Getter
    private final Map<Long, MatrixStore<Double>> remoteRsByProcessStep = new HashMap<>();
    @Setter @Getter
    private LocalDateTime startTime;
    @Setter @Getter
    private LocalDateTime endTime;
}
