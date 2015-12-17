package io.paradoxical.cassandra.leadership;


import com.datastax.driver.core.Session;
import com.godaddy.logging.Logger;
import io.paradoxical.cassandra.leadership.data.LeaderIdentity;
import io.paradoxical.cassandra.leadership.data.LeaderStatus;
import io.paradoxical.cassandra.leadership.data.LeadershipGroup;
import io.paradoxical.cassandra.leadership.data.LeadershipSchema;
import io.paradoxical.cassandra.leadership.data.LeadershipToken;
import io.paradoxical.cassandra.leadership.factories.CassandraLeadershipElectionFactory;
import io.paradoxical.cassandra.leadership.interfaces.LeadershipElection;
import io.paradoxical.cassandra.leadership.interfaces.LeadershipElectionFactory;
import io.paradoxical.cassandra.leadership.interfaces.LeadershipStatus;
import io.paradoxical.cassandra.loader.db.CqlUnitDb;
import io.paradoxical.common.test.junit.RetryRule;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Optional;

import static com.godaddy.logging.LoggerFactory.getLogger;
import static org.assertj.core.api.Assertions.assertThat;

public class LeadershipStorageTests extends TestBase {
    private static final Logger logger = getLogger(LeadershipStorageTests.class);

    @Rule
    public RetryRule retrier = new RetryRule(3);

    private static Session session;

    static {
        try {
            session = CqlUnitDb.create("db");
        }
        catch (Exception e) {
            logger.error(e, "Error creating db");
        }
    }

    @Test
    public void test_leadership_newly_elected() throws Exception {
        final LeadershipElection leadership = getLeadershipFactory().create(LeadershipGroup.random());

        assertThat(leadership.tryClaimLeader(LeaderIdentity.valueOf("user1"), Duration.ofSeconds(3))).isPresent();
    }

    @Test
    public void test_leadership_newly_elected_wont_relinquish() throws Exception {
        final LeadershipElection leadership = getLeadershipFactory().create(LeadershipGroup.random());

        final LeaderIdentity user1 = LeaderIdentity.valueOf("user1");

        final LeaderIdentity user2 = LeaderIdentity.valueOf("test_leadership_newly_elected_wont_relinquish_user2");

        assertThat(leadership.tryClaimLeader(user1, Duration.ofSeconds(3))).isPresent();

        assertThat(leadership.tryClaimLeader(user2, Duration.ofSeconds(3))).isEmpty();
    }

    @Test
    public void test_leadership_newly_elected_ttl_expires() throws Exception {
        final LeadershipElection leadership = getLeadershipFactory().create(LeadershipGroup.random());

        final LeaderIdentity user1 = LeaderIdentity.valueOf("user1");

        final LeaderIdentity user2 = LeaderIdentity.valueOf("test_leadership_newly_elected_ttl_expires_user2");

        assertThat(leadership.tryClaimLeader(user1, Duration.ofSeconds(2))).isPresent();

        Thread.sleep(Duration.ofSeconds(3).toMillis());

        assertThat(leadership.tryClaimLeader(user2, Duration.ofSeconds(3))).isPresent();
    }

    @Test
    public void leadership_is_reentrant() throws Exception {
        final LeadershipElection leadership = getLeadershipFactory().create(LeadershipGroup.random());

        final LeaderIdentity user1 = LeaderIdentity.valueOf("user1");

        final LeaderIdentity user2 = LeaderIdentity.valueOf("leadership_is_reentrant_user3");

        final Duration timeToLive = Duration.ofSeconds(3);

        assertThat(leadership.tryClaimLeader(user1, timeToLive)).isPresent();

        assertThat(leadership.tryClaimLeader(user2, Duration.ofSeconds(1))).isEmpty();

        assertThat(leadership.tryClaimLeader(user1, timeToLive)).isPresent();

        assertThat(leadership.tryClaimLeader(user1, timeToLive)).isPresent();

        Thread.sleep(3500);

        assertThat(leadership.tryClaimLeader(user2, Duration.ofSeconds(1))).isPresent();
    }

    @Test
    public void leadership_heartbeats_updates_ttl() throws Exception {
        final LeadershipElection leadership = getLeadershipFactory().create(LeadershipGroup.random());

        final LeaderIdentity user1 = LeaderIdentity.valueOf("user1");

        final LeaderIdentity user2 = LeaderIdentity.valueOf("leadership_heartbeats_updates_ttl_user2");

        final Duration timeToLive = Duration.ofSeconds(2);

        final LocalTime start = LocalTime.now();

        // heartbeat twice the ttl
        final LocalTime end = start.plus(timeToLive).plus(timeToLive);

        final LeadershipToken claim = leadership.tryClaimLeader(user1, timeToLive).get();

        while (LocalTime.now().isBefore(end)) {
            logger.info("Heartbeat");

            assertThat(leadership.tryClaimLeader(user2, timeToLive)).isEmpty();

            assertThat(leadership.tryHeartbeat(claim)).isTrue();

            assertThat(leadership.getLeader()).isEqualTo(user1);

            Thread.sleep(500);
        }

        Thread.sleep(3000);

        assertThat(leadership.tryClaimLeader(user2, Duration.ofSeconds(5))).isPresent();

        assertThat(leadership.getLeader()).isEqualTo(user2);
    }

    @Test
    public void heartbeat_on_not_leader_claim_should_be_false() throws Exception {
        final LeadershipElection leadership = getLeadershipFactory().create(LeadershipGroup.random());

        final LeaderIdentity user1 = LeaderIdentity.valueOf("user1");

        final Duration timeToLive = Duration.ofSeconds(1);

        final LeadershipToken claim = leadership.tryClaimLeader(user1, timeToLive).get();

        Thread.sleep(2000);

        assertThat(leadership.tryHeartbeat(claim)).isFalse();
    }

    @Test
    public void renounces_leadership_makes_leadership_available() throws Exception {
        final LeadershipElection leadership = getLeadershipFactory().create(LeadershipGroup.random());

        final LeaderIdentity user1 = LeaderIdentity.valueOf("user1");
        final LeaderIdentity user2 = LeaderIdentity.valueOf("user2");

        final Duration timeToLive = Duration.ofSeconds(100);

        final LeadershipToken claim = leadership.tryClaimLeader(user1, timeToLive).get();

        assertThat(leadership.renounceLeadership(claim)).isTrue();

        assertThat(leadership.tryClaimLeader(user2, timeToLive)).isPresent();
    }

    @Test
    public void test_leadership_status_ticks_down_and_expires() throws InterruptedException {
        final LeadershipGroup group = LeadershipGroup.random();
        final LeadershipElection leadership = getLeadershipFactory().create(group);

        final LeaderIdentity user1 = LeaderIdentity.valueOf("user1");

        final Duration timeToLive = Duration.ofSeconds(3);

        leadership.tryClaimLeader(user1, timeToLive);

        final LeadershipStatus leaderListStorage = new LeadershipStatusImpl(session, LeadershipSchema.Default);

        final LocalTime start = LocalTime.now();

        final LocalTime end = start.plus(timeToLive).plus(timeToLive);

        leadership.tryClaimLeader(user1, timeToLive).get();

        int ttl = Integer.MAX_VALUE;

        while (LocalTime.now().isBefore(end)) {
            final Optional<LeaderStatus> leaderStatus = leaderListStorage.listLeaders().stream().filter(i -> i.getGroup().equals(group)).findFirst();

            if (!leaderStatus.isPresent()) {
                break;
            }

            assertThat(leaderStatus.get().getRemainingTtlSeconds()).isLessThan(ttl);

            System.out.println(leaderStatus);

            ttl = leaderStatus.get().getRemainingTtlSeconds();

            Thread.sleep(1500);
        }

        assertThat(ttl).isLessThan(Integer.MAX_VALUE);

        assertThat(leaderListStorage.listLeaders().stream().filter(i -> i.getGroup().equals(group)).findFirst()).isEmpty();
    }

    private LeadershipElectionFactory getLeadershipFactory() {
        return new CassandraLeadershipElectionFactory(session);
    }
}
