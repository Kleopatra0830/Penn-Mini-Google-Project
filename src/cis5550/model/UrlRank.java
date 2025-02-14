package cis5550.model;

import cis5550.flame.FlamePair;

public class UrlRank implements Comparable<UrlRank> {

    private final String url;
    private final double tfidf;
    private final double pagerank;
    private double score;
    private boolean isCaculated;
    private final double additionalScore;

    public UrlRank(String url, double tfidf, double pagerank, double additionalScore) {
        this.url = url;
        this.tfidf = tfidf;
        this.pagerank = pagerank;
//        this.score = 0;
        this.isCaculated = false;
        this.additionalScore = additionalScore;
    }

    public double calculateScore() {
        if (isCaculated) {
            return score;
        }
        score = tfidf * pagerank;
        isCaculated = true;
//        System.out.println(url + " tfidf: " + tfidf + ", pagerank: " + pagerank + ", score: " + score);
        return score + additionalScore;
    }

    @Override
    public int compareTo(UrlRank o) {
        return Double.compare(this.score, o.score);
    }

    @Override
    public String toString() {
        return url + " " + score;
    }

    public FlamePair toFlamePair() {
        return new FlamePair(url, tfidf + "," + pagerank + "," + score);
    }

    public String getUrl() {
        return url;
    }
}
