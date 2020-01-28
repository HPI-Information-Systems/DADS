package de.hpi.msc.jschneider.math;

import junit.framework.TestCase;
import lombok.val;
import smile.stat.distribution.KernelDensity;

public class SmileLearningTest extends TestCase
{
    public void testGaussianKernelDensityEstimation()
    {
        val samples = new double[]{0.0d, 1.0d, 2.0d, 3.0d, 4.0d, 5.0d, 6.0d};
        val scottsFactor = Calculate.scottsFactor(samples.length, 1L);
        val kde = new KernelDensity(samples, scottsFactor);

        kde.p(0.5d);
    }
}
