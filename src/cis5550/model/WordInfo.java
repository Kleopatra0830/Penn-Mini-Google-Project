package cis5550.model;

import java.io.Serializable;

public record WordInfo(String word, long count) implements Serializable {
    public WordInfo(String word, long count) {
        this.word = word.toLowerCase();
        this.count = count;
    }
}
