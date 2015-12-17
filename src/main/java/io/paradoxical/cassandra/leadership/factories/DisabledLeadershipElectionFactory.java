package io.paradoxical.cassandra.leadership.factories;

import io.paradoxical.cassandra.leadership.data.LeaderIdentity;
import io.paradoxical.cassandra.leadership.data.LeadershipToken;
import io.paradoxical.cassandra.leadership.interfaces.LeadershipElectionFactory;
import io.paradoxical.cassandra.leadership.data.LeadershipGroup;
import io.paradoxical.cassandra.leadership.interfaces.LeadershipElection;

import java.time.Duration;
import java.util.Optional;

public class DisabledLeadershipElectionFactory implements LeadershipElectionFactory {
    @Override public LeadershipElection create(final LeadershipGroup key) {
        return new LeadershipElection() {

            /**
             * Everyone can be leader
             * @param identity
             * @param ttl
             * @return
             */
            @Override public Optional<LeadershipToken> tryClaimLeader(
                    final LeaderIdentity identity, final Duration ttl) {
                return Optional.of(new LeadershipToken(ttl, identity, null));
            }

            /**
             * All heartbeats succeed
             * @param claim
             * @return
             */
            @Override public boolean tryHeartbeat(final LeadershipToken claim) {
                return true;
            }

            /**
             * All renounces succeed
             * @param claim
             * @return
             */
            @Override public boolean renounceLeadership(final LeadershipToken claim) {
                return true;
            }

            @Override public LeaderIdentity getLeader() {
                return null;
            }
        };
    }
}
