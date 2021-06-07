package com.home.dedup;

import com.home.models.Tweet;
import org.junit.jupiter.api.Test;

import java.time.Instant;

public class SimpleSerdeTest {

    @Test
    public void TestTweetSerde() {

        Tweet t = new Tweet(10L, "10", Instant.now());
        String s = t.Serialize();
        System.out.println(s);
        Tweet n = Tweet.Deserialize(s);
        assert t.equals(n);

    }
}
