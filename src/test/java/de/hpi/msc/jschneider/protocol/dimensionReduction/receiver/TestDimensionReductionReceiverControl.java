package de.hpi.msc.jschneider.protocol.dimensionReduction.receiver;

import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionEvents;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.val;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDimensionReductionReceiverControl extends ProtocolTestCase
{
    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.DimensionReduction};
    }

    private DimensionReductionReceiverModel dummyModel()
    {
        return finalizeModel(DimensionReductionReceiverModel.builder().build());
    }

    private DimensionReductionReceiverControl control()
    {
        return new DimensionReductionReceiverControl(dummyModel());
    }

    public void testAcceptPrincipalComponentsTransfer()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val message = DimensionReductionMessages.InitializePrincipalComponentsTransferMessage.builder()
                                                                                             .sender(self.ref())
                                                                                             .receiver(self.ref())
                                                                                             .operationId(UUID.randomUUID().toString())
                                                                                             .build();
        messageInterface.apply(message);

        val requestNextPart = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
        assertThat(requestNextPart.getOperationId()).isEqualTo(message.getOperationId());

        assertThatMessageIsCompleted(message);
    }

    public void testAcceptRotationTransfer()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val message = DimensionReductionMessages.InitializeRotationTransferMessage.builder()
                                                                                  .sender(self.ref())
                                                                                  .receiver(self.ref())
                                                                                  .operationId(UUID.randomUUID().toString())
                                                                                  .build();
        messageInterface.apply(message);

        val requestNextPart = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
        assertThat(requestNextPart.getOperationId()).isEqualTo(message.getOperationId());

        assertThatMessageIsCompleted(message);
    }

    public void testAcceptColumnMeansTransfer()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val message = DimensionReductionMessages.InitializeColumnMeansTransferMessage.builder()
                                                                                     .sender(self.ref())
                                                                                     .receiver(self.ref())
                                                                                     .operationId(UUID.randomUUID().toString())
                                                                                     .numberOfColumns(5)
                                                                                     .build();
        messageInterface.apply(message);

        val requestNextPart = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
        assertThat(requestNextPart.getOperationId()).isEqualTo(message.getOperationId());

        assertThatMessageIsCompleted(message);
    }

    public void testWaitForBothTransfersBeforePerformingDimensionReduction()
    {
        val principalComponents = createMatrix(5, 3);
        val rotation = createMatrix(3, 3);
        val projection = createMatrix(100, 5);
        val columnMeans = createMatrix(1, 5);

        val control = control();
        control.getModel().setProjection(projection);
        val messageInterface = createMessageInterface(control);

        val initializePrincipalComponentsTransfer = DimensionReductionMessages.InitializePrincipalComponentsTransferMessage.builder()
                                                                                                                           .sender(self.ref())
                                                                                                                           .receiver(self.ref())
                                                                                                                           .operationId(UUID.randomUUID().toString())
                                                                                                                           .build();
        transfer(principalComponents, self, messageInterface, initializePrincipalComponentsTransfer, true);
        assertThat(control.getModel().getPrincipalComponents().equals(principalComponents, MATRIX_COMPARISON_CONTEXT)).isTrue();

        val initializeRotationTransfer = DimensionReductionMessages.InitializeRotationTransferMessage.builder()
                                                                                                     .sender(self.ref())
                                                                                                     .receiver(self.ref())
                                                                                                     .operationId(UUID.randomUUID().toString())
                                                                                                     .build();
        transfer(rotation, self, messageInterface, initializeRotationTransfer, true);
        assertThat(control.getModel().getRotation().equals(rotation, MATRIX_COMPARISON_CONTEXT)).isTrue();

        val initializeColumnMeansTransfer = DimensionReductionMessages.InitializeColumnMeansTransferMessage.builder()
                                                                                                           .sender(self.ref())
                                                                                                           .receiver(self.ref())
                                                                                                           .operationId(UUID.randomUUID().toString())
                                                                                                           .numberOfColumns(columnMeans.countColumns())
                                                                                                           .build();
        transfer(columnMeans, self, messageInterface, initializeColumnMeansTransfer, false);

        val projection2d = rotation.multiply(projection.subtract(columnMeans).multiply(principalComponents).transpose()).logical().row(0, 1).get();
        val reducedProjectionCreatedEvent = expectEvent(DimensionReductionEvents.ReducedProjectionCreatedEvent.class);

        assertThat(projection2d.equals(reducedProjectionCreatedEvent.getReducedProjection(), MATRIX_COMPARISON_CONTEXT)).isTrue();
    }
}
