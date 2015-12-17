package io.paradoxical.cassandra.leadership.data;

import lombok.Value;

@Value
public class LeaderStatus {
    private LeadershipGroup group;
    private LeaderIdentity activeLeader;
    private int remainingTtlSeconds;
}
