package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.calculator;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.PrimitiveMatrix;

import java.util.Map;

@SuperBuilder
public class PCACalculatorModel extends AbstractProtocolParticipantModel
{
    @Getter @Setter
    private Map<RootActorPath, Integer> processorIndices;
    @Getter @Setter
    private int myProcessorIndex;
    @Getter @Setter
    private PrimitiveMatrix projection;
}
