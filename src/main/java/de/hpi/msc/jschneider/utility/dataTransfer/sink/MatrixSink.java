package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import de.hpi.msc.jschneider.utility.dataTransfer.DataSink;
import org.ojalgo.matrix.store.MatrixStore;

public interface MatrixSink extends DataSink
{
    MatrixStore<Double> getMatrix();
}
