package de.hpi.msc.jschneider.utility.matrix;

import org.ojalgo.matrix.store.MatrixStore;

public interface MatrixBuilder
{
    MatrixBuilder append(double[] values);

    MatrixStore<Double> build();
}
