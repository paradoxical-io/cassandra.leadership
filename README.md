cassandra-leadership
========================

![Build status](https://travis-ci.org/paradoxical-io/cassandra.leadership.svg?branch=master)

A library to help elect leaders using cassandra.  Cassandra is [already leveraging paxos](http://www.datastax.com/dev/blog/consensus-on-cassandra), and with 
cassandra's expiring columns we can build a simple leadership election module. 

## Installation

To install

```
<dependency>
    <groupId>io.paradoxical</groupId>
    <artifactId>cassandra-leadership</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

## Usage

First you need some sort of leadership election tracking table. You can custom the schema names, but you must have something like:

```
CREATE TABLE leadership_election (
    group text PRIMARY KEY,
    leader_id text
);
```

In order for this to work. The default schema uses the one above.

```
LeadershipSchema.Default
```

### Election

Once you have your leadership tables created you can create a leadership election factory.  The factory takes a `LeadershipGroup` 
as a key. This lets you create leadership _per group_. For example, if you want to elect the leader for a group of workers, 
pin all the workers to the same leadership group. Each worker should have its own unique ID and it will be elected leader.

While doing work, if you want to maintain leadership status you will need to heartbeat. It is recommended to heartbeat half the ttl.
 
For example, if you set the lease time TTL to be 30 seconds, you should attempt to heartbeat no later than every 15 seconds.
  
Leadership here is re-entrant. If a leader is already active and that same leader asks to elect a leader it will return itself until either
its lease time is up, or it explicitly renounces leadership.

As an example

```
final LeadershipElection leadership = new CassandraLeadershipElectionFactory(session).create(LeadershipGroup.random());

final LeaderIdentity user1 = LeaderIdentity.valueOf("user1");

final LeaderIdentity user2 = LeaderIdentity.valueOf("user2");

assertThat(leadership.tryClaimLeader(user1, Duration.ofSeconds(2))).isPresent();

Thread.sleep(Duration.ofSeconds(3).toMillis());

assertThat(leadership.tryClaimLeader(user2, Duration.ofSeconds(3))).isPresent();
```

### Listing current leader groups

Listing current leader groups is provided for via the `LeadershipStatus` contract which will list you the current group and its leader id.  It also gives 
you the amount of seconds that the leader will be active for.  When the leader is no longer active the entry is removed.