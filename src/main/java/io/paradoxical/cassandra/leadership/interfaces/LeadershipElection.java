package io.paradoxical.cassandra.leadership.interfaces;

import io.paradoxical.cassandra.leadership.data.LeaderIdentity;
import io.paradoxical.cassandra.leadership.data.LeadershipToken;

import java.time.Duration;
import java.util.Optional;

public interface LeadershipElection {
    Optional<LeadershipToken> tryClaimLeader(LeaderIdentity identity, Duration ttl);

    boolean tryHeartbeat(LeadershipToken claim);

    boolean renounceLeadership(LeadershipToken claim);

    LeaderIdentity getLeader();
}

