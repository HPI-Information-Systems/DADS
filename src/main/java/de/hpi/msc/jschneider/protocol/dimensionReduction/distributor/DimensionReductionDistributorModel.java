package de.hpi.msc.jschneider.protocol.dimensionReduction.distributor;

import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.store.MatrixStore;

@SuperBuilder
public class DimensionReductionDistributorModel extends AbstractProtocolParticipantModel
{
    @Getter
    private Protocol receiverProtocol;
    @Getter
    private MatrixStore<Double> rotation;
    @Getter
    private MatrixStore<Double> principalComponents;
}
