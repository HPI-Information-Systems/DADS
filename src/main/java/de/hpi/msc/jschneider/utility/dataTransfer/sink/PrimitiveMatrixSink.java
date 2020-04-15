package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.SneakyThrows;
import lombok.val;
import org.ojalgo.matrix.Primitive64Matrix;
import org.ojalgo.matrix.store.MatrixStore;

public class PrimitiveMatrixSink implements MatrixSink
{
    private Primitive64Matrix.DenseReceiver matrixReceiver;
    private MatrixStore<Double> matrix;
    private long matrixReceiverIndex;

    @SneakyThrows
    @Override
    public void synchronize(DataTransferMessages.DataTransferSynchronizationMessage message)
    {
        if (!(message instanceof DataTransferMessages.MatrixTransferSynchronizationMessage))
        {
            throw new Exception(String.format("Expected %1$s, but got %2$s!",
                                              DataTransferMessages.MatrixTransferSynchronizationMessage.class.getSimpleName(),
                                              message.getClass().getSimpleName()));
        }

        val synchronizationMessage = (DataTransferMessages.MatrixTransferSynchronizationMessage) message;
        matrixReceiver = Primitive64Matrix.FACTORY.makeDense(synchronizationMessage.getNumberOfRows(), synchronizationMessage.getNumberOfColumns());
        matrixReceiverIndex = 0L;
    }

    @Override
    public void write(byte[] part, int partLength)
    {
        matrixReceiverIndex = Serialize.backInPlace(part, partLength, matrixReceiver, matrixReceiverIndex);
    }

    @Override
    public void close()
    {
        matrix = MatrixStore.PRIMITIVE64.makeWrapper(matrixReceiver.build()).get();
    }

    @Override
    public MatrixStore<Double> getMatrix()
    {
        return matrix;
    }
}
