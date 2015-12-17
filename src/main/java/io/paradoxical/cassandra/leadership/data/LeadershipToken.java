package io.paradoxical.cassandra.leadership.data;

import lombok.Value;

import java.time.Duration;

@Value
public class LeadershipToken {
    private Duration ttl;

    private LeaderIdentity identity;

    private LeadershipGroup roleId;
}
