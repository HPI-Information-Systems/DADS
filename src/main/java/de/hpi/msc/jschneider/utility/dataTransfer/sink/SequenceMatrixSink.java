package de.hpi.msc.jschneider.utility.dataTransfer.sink;

public interface SequenceMatrixSink extends MatrixSink
{
    double getMinimumRecord();

    double getMaximumRecord();
}
