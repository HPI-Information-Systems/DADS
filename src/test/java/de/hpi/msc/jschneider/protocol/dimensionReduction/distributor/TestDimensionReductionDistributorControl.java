package de.hpi.msc.jschneider.protocol.dimensionReduction.distributor;

import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.TestProcessor;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionMessages;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDimensionReductionDistributorControl extends ProtocolTestCase
{
    private TestProcessor remoteProcessor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        remoteProcessor = createProcessor("remote", false);
    }

    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.DimensionReduction};
    }

    public void testStartDistributionToSelf()
    {
        val principalComponents = createMatrix(5, 3); // reduced 5 dimensions to 3
        val rotation = createMatrix(3, 3);
        val columnMeans = createMatrix(1, 5);
        val model = finalizeModel(DimensionReductionDistributorModel.builder()
                                                                    .receiverProtocol(localProcessor.getProtocol(ProtocolType.DimensionReduction))
                                                                    .principalComponents(principalComponents)
                                                                    .rotation(rotation)
                                                                    .columnMeans(columnMeans)
                                                                    .build());
        val control = new DimensionReductionDistributorControl(model);

        control.preStart();

        val initializeRotationTransfer = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DimensionReductionMessages.InitializeRotationTransferMessage.class);
        assertThat(initializeRotationTransfer.getReceiver()).isEqualTo(localProcessor.getProtocolRootActor(ProtocolType.DimensionReduction).ref());

        val initializePrincipalComponentsTransfer = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DimensionReductionMessages.InitializePrincipalComponentsTransferMessage.class);
        assertThat(initializePrincipalComponentsTransfer.getReceiver()).isEqualTo(localProcessor.getProtocolRootActor(ProtocolType.DimensionReduction).ref());

        val initializeColumnMeansTransfer = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DimensionReductionMessages.InitializeColumnMeansTransferMessage.class);
        assertThat(initializeColumnMeansTransfer.getReceiver()).isEqualTo(localProcessor.getProtocolRootActor(ProtocolType.DimensionReduction).ref());
    }

    public void testStartDistributionToRemote()
    {
        val principalComponents = createMatrix(5, 3); // reduced 5 dimensions to 3
        val rotation = createMatrix(3, 3);
        val columnMeans = createMatrix(1, 5);
        val model = finalizeModel(DimensionReductionDistributorModel.builder()
                                                                    .receiverProtocol(remoteProcessor.getProtocol(ProtocolType.DimensionReduction))
                                                                    .principalComponents(principalComponents)
                                                                    .rotation(rotation)
                                                                    .columnMeans(columnMeans)
                                                                    .build());
        val control = new DimensionReductionDistributorControl(model);

        control.preStart();

        val initializeRotationTransfer = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DimensionReductionMessages.InitializeRotationTransferMessage.class);
        assertThat(initializeRotationTransfer.getReceiver()).isEqualTo(remoteProcessor.getProtocolRootActor(ProtocolType.DimensionReduction).ref());

        val initializePrincipalComponentsTransfer = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DimensionReductionMessages.InitializePrincipalComponentsTransferMessage.class);
        assertThat(initializePrincipalComponentsTransfer.getReceiver()).isEqualTo(remoteProcessor.getProtocolRootActor(ProtocolType.DimensionReduction).ref());

        val initializeColumnMeansTransfer = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DimensionReductionMessages.InitializeColumnMeansTransferMessage.class);
        assertThat(initializeColumnMeansTransfer.getReceiver()).isEqualTo(remoteProcessor.getProtocolRootActor(ProtocolType.DimensionReduction).ref());
    }
}
