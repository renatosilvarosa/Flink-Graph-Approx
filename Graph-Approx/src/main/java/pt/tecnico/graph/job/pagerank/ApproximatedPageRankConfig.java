package pt.tecnico.graph.job.pagerank;

/**
 * Created by Renato on 29/06/2016.
 */
public class ApproximatedPageRankConfig {
    private int iterations;
    private double beta;
    private double updatedRatioThreshold;
    private long updatedNumberThreshold;
    private int neighborhoodSize;
    private boolean printRanks;
    private int outputSize;
    private int maximumApproximatedComputations;
    private double maximumSampleRelativeSize;


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

    public boolean isPrintRanks() {
        return printRanks;
    }

    public ApproximatedPageRankConfig setPrintRanks(boolean printRanks) {
        this.printRanks = printRanks;
        return this;
    }

    public int getMaximumApproximatedComputations() {
        return maximumApproximatedComputations;
    }

    public ApproximatedPageRankConfig setMaximumApproximatedComputations(int maximumApproximatedComputations) {
        this.maximumApproximatedComputations = maximumApproximatedComputations;
        return this;
    }

    public double getMaximumSampleRelativeSize() {
        return maximumSampleRelativeSize;
    }

    public ApproximatedPageRankConfig setMaximumSampleRelativeSize(double maximumSampleRelativeSize) {
        this.maximumSampleRelativeSize = maximumSampleRelativeSize;
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
