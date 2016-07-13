package pt.tecnico.graph.algorithm.pagerank;

/**
 * Created by Renato on 29/06/2016.
 */
public class ApproximatedPageRankConfig {
    private int iterations = 20;
    private double beta = 0.85;
    private double updatedRatioThreshold = 0.0;
    private long updatedNumberThreshold = 0;
    private int neighborhoodSize = 1;
    private int outputSize = 1000;


    public int getIterations() {
        return iterations;
    }

    public ApproximatedPageRankConfig setIterations(int iterations) {
        this.iterations = iterations;
        return this;
    }

    public double getBeta() {
        return beta;
    }

    public ApproximatedPageRankConfig setBeta(double beta) {
        this.beta = beta;
        return this;
    }

    public double getUpdatedRatioThreshold() {
        return updatedRatioThreshold;
    }

    public ApproximatedPageRankConfig setUpdatedRatioThreshold(double updatedRatioThreshold) {
        this.updatedRatioThreshold = updatedRatioThreshold;
        return this;
    }

    public long getUpdatedNumberThreshold() {
        return updatedNumberThreshold;
    }

    public ApproximatedPageRankConfig setUpdatedNumberThreshold(long updatedNumberThreshold) {
        this.updatedNumberThreshold = updatedNumberThreshold;
        return this;
    }

    public int getNeighborhoodSize() {
        return neighborhoodSize;
    }

    public ApproximatedPageRankConfig setNeighborhoodSize(int neighborhoodSize) {
        this.neighborhoodSize = neighborhoodSize;
        return this;
    }

    public int getOutputSize() {
        return outputSize;
    }

    public ApproximatedPageRankConfig setOutputSize(int outputSize) {
        this.outputSize = outputSize;
        return this;
    }
}
