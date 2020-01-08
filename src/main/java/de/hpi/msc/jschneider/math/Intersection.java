package de.hpi.msc.jschneider.math;

import lombok.Builder;
import lombok.Getter;
import org.ojalgo.matrix.store.MatrixStore;

@Builder @Getter
public class Intersection
{
    private MatrixStore<Double> vector;
    private double vectorLength;
}
