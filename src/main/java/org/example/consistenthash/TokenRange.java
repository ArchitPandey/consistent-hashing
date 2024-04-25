package org.example.consistenthash;

public class TokenRange {

    private int startToken;

    private int endToken;

    public TokenRange(int start, int end) {
        this.startToken = start;
        this.endToken = end;
    }

    public int getStartToken() {
        return this.startToken;
    }

    public int getEndToken() {
        return this.endToken;
    }

    public String toString() {
        return "StartToken:".concat(Integer.toString(startToken)).concat(" - EndToken:").concat(Integer.toString(endToken));
    }
}
