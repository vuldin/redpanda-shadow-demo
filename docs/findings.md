# Bidirectional Shadow Links: what works, what doesn't

Verified locally against `docker.redpanda.com/redpandadata/redpanda:v26.2.2` +
`console:v3.11.0` (2026-09-15), using this repo's docker-compose environment.
Also confirmed on v25.3.1 - behavior is identical across both.

## Setup

Two independent shadow links, one in each direction:

- `demo-shadow-link` - created ON `redpanda-shadow`, source = `redpanda-source`.
  Shadows topics matching prefix `demo-`.
- `reverse-shadow-link` - created ON `redpanda-source`, source = `redpanda-shadow`.
  Shadows topics matching prefix `back-`.

Each cluster is the *destination* of exactly one link. That's the hard
constraint Shadowing enforces (a destination cluster can only maintain one
shadow link), and it's why "bidirectional" here means **two separate links
pointed at each other**, not one link with a `bidirectional: true` flag -
no such flag exists.

## Result 1: Bidirectional works, as long as topic namespaces don't overlap

With `demo-*` flowing source -> shadow and `back-*` flowing shadow -> source,
both links reach `STATE ACTIVE` simultaneously and real data replicates both
ways with zero lag in each direction. Nothing about running two links at once,
in opposite directions, between the same two clusters is disallowed or flaky.

```
make setup && make setup-reverse
```

confirms this end to end (see `scripts/setup-shadow-link.sh` +
`scripts/setup-reverse-shadow-link.sh`).

## Result 2: Overlapping topics don't error - they silently no-op

This is the important one. A shadow link never overwrites, merges, or fights
over a topic name. Its topic auto-creation step
(`source_topic_syncer::find_candidate_topics_for_creation`) skips any source
topic that **already exists locally on the destination cluster** - full stop,
no matter which direction shadowed it there or whether it's a "real" topic or
a leftover from an earlier link.

Consequence for a naive "just mirror everything both ways" config
(`auto_create_shadow_topic_filters: ['*']` on both links): if a topic name
already exists on *both* clusters - which will always be true for any topic
that started life natively on one side and was never shadowed, plus anything
promoted by a prior failover - **neither link will shadow it**. Both links
still report `STATE ACTIVE`. There is no error, no `FAULTED` state, no log
warning visible from `rpk shadow status`. The link just quietly shadows
nothing:

```
TOPICS
======
No topics are being shadowed.
```

We reproduced this exactly: `demo-events`, `demo-metrics`, `back-events`, and
a deliberately-collided `clash-orders` topic (independently created with
different data on each cluster) all ended up existing natively on both
clusters, then both wildcard links came up `ACTIVE` and shadowed **zero**
topics between them. See `scripts/test-overlap.sh` for the reproduction and
`config/shadow-link-wildcard.yaml` / `config/shadow-link-reverse-wildcard.yaml`
for the configs.

**Operational implication:** a link showing `STATE ACTIVE` proves nothing
about whether it's replicating anything. Always check the `TOPICS` section
of `rpk shadow status` (or the dataplane `GET /v1/shadow-links/{name}/topic`
in Cloud) for non-empty `shadow_topics` + `LAG` converging to 0. This matches
the existing internal guidance from failover-recovery work - it's the same
underlying mechanism, just triggered by a fresh bidirectional setup instead
of a failover-and-recreate.

## Result 3: New topics after the fact are picked up one-directionally, with no loop

This is what makes overlapping-filter bidirectional links *safe* rather than
just broken: we created a brand-new topic (`fresh-alpha`) on the source
cluster only, after both wildcard links were already active. The forward
link picked it up cleanly (`STATE ACTIVE`, lag 0). The reverse link - whose
filter also matches `fresh-alpha` and which polls the *shadow* cluster where
`fresh-alpha` now also exists as a shadow copy - never tried to shadow it
back to source, because by the time its sync interval ran, `fresh-alpha`
already existed locally on the source (the original). No infinite
back-and-forth, no duplication, no corruption.

So the collision rule is also what prevents a naive bidirectional mirror
from turning into a replication loop. The cost is that it does so silently,
which is the actual gotcha to plan around, not a "will it loop" risk.

## Result 4: filter syntax - PREFIX means literal prefix, no `*` character allowed

`auto_create_shadow_topic_filters` only has two `pattern_type`s (confirmed via
`rpk shadow config generate` - no REGEX/glob type exists):

- `LITERAL` with `name: '*'` - the one and only "match everything" wildcard.
  This is a special-cased literal string (the same convention Kafka ACLs use
  for `LITERAL '*'`), not a glob character.
- `PREFIX` with `name: <bare prefix string>` - matches any topic name
  starting with that string. **Do not include a trailing `*`.** `PREFIX`
  already means "starts with"; the asterisk is implied, not typed.

So the answer to "can each direction use its own prefix, like `prefixa_*` one
way and `prefixb_*` the other" is: yes, that's exactly this demo's `demo-` /
`back-` setup - but write it as `name: prefixa_` and `name: prefixb_`, no
star. We tested the literal-star spelling directly:

```yaml
- pattern_type: PREFIX
  filter_type: INCLUDE
  name: 'prefixa_*'   # WRONG
```

This doesn't get silently ignored or treated as a no-op filter - the control
plane validates filters at shadow-link **creation** time and rejects the
entire request:

```
unable to create shadow link: invalid_argument: ... Bad Request,
Failed to create cluster link: cluster::cluster_link::errc::topic_filter_invalid
```

Importantly, one bad filter entry fails the *whole* `create` call, even if
other filters in the same list (e.g. `back-`) are valid. Dropping the `*`
(`name: prefixb_`) creates cleanly and shadows matching topics with lag 0.

## Result 5: a topic created after the link is already active gets picked up automatically, with full backfill

The filter isn't a one-time thing evaluated at link-creation time - `topic_metadata_sync_options.interval` (10s in this demo) keeps re-checking the source cluster's topic list against the filter on every tick. We created `demo-newarrival` (2 partitions) on the source *after* `demo-shadow-link` was already `ACTIVE`, and produced 5 messages to it immediately - before the shadow cluster had any record of it (`rpk topic list` on shadow showed nothing right after creation).

One sync interval later:
- `demo-newarrival` appeared on the shadow cluster automatically, no link update/recreate needed.
- Both partitions backfilled fully: `SRC-LSO`/`SRC-HWM`/`DST-HWM` matched exactly (3 and 2 messages per partition) and all 5 message values round-tripped correctly.
- Lag converged to 0 immediately after discovery.

This is because `start_at_earliest: {}` seeds a newly-discovered shadow topic from the source's earliest offset, not from "now." A topic that starts matching a prefix filter after the link is already running behaves identically to one that existed before the link was created - full history included, not just subsequent writes. Partition count and synced properties also carry over automatically.

**Confirmed symmetric in the reverse direction too.** With both `demo-shadow-link` and `reverse-shadow-link` already `ACTIVE`, we created `back-newarrival` (2 partitions, 4 messages) directly on `redpanda-shadow` - `reverse-shadow-link`'s source - after the link was already running. Same result: not visible on `redpanda-source` immediately, then one sync interval later both partitions backfilled fully (lag 0) and all 4 messages round-tripped correctly. So this isn't a forward-link-only behavior - it holds for whichever cluster is acting as a link's source, in either direction of a bidirectional setup.

## Result 6: shadow topic creation does not depend on `auto_create_topics_enabled`

Redpanda's Kafka-compatible `auto_create_topics_enabled` cluster property (note the trailing "d" - not Kafka's exact spelling) controls whether a *client* implicitly creates a topic by producing to / fetching metadata for a name that doesn't exist yet. This is a completely different mechanism from Shadow Link's own topic auto-creation.

We set `auto_create_topics_enabled: false` on the destination (`redpanda-shadow`), then created a brand-new prefix-matching topic on the source. The shadow link still created the shadow copy on the destination and fully replicated it (lag 0) on the very next sync interval - `auto_create_topics_enabled` on the destination had zero effect. Shadow topic creation goes through the link's own internal admin path (`Source Topic Sync` task), not the Kafka-protocol implicit-create path that property gates.

Note also from the docs: `auto_create_topics_enabled` "is not supported on Redpanda Cloud BYOC and Dedicated clusters" at all - another reason it's unrelated to whether Shadowing can create topics on Cloud.

**Side finding while cleaning up this test:** a shadow (auto-mirrored) topic cannot be deleted directly - `rpk topic delete` on it returns `POLICY_VIOLATION: Auto-mirrored topic cannot be deleted`. You have to fail it over first (`rpk shadow failover <link> --topic <topic>`, or `--all` for the whole link) before it becomes an ordinary, deletable topic. In our test, even after `rpk shadow status` showed the topic as `FAILED_OVER`, direct deletion still failed for a while - the reliable path was `rpk shadow delete <link> --force` (which fails over and detaches all of that link's topics at once), then delete, then recreate the link.

## Result 7: SASL/security - ACLs (sometimes) replicate, credentials never do

Three separate questions, answered by direct testing with SASL/SCRAM users and ACLs on both clusters (`enable_sasl` stayed `false` throughout - only user/ACL *metadata* was exercised, not live authentication):

**Does SASL get replicated?** Only ACL *bindings* can be replicated, via `security_sync_options.acl_filters` (the `Security Migrator Task` in `rpk shadow status`). The actual SASL/SCRAM **credential** (the username+password secret) is never replicated, under any configuration we tried. We created user `alice` with a password and a matching ACL on the source; after fixing ACL replication (see below), `alice`'s ACL showed up on the destination but the user account itself never did - `rpk security user list` on the destination stayed empty the whole time. Users/credentials and ACLs are different stores in Redpanda, and Shadowing only touches the ACL store.

**Getting ACL replication to actually work is easy to get wrong, and it fails silently:**
1. `access_filter.principal` is a literal `PrincipalType:name` string, not a filter pattern. This demo originally shipped with `principal: '*'` (bare wildcard) as an example - **this is broken**. The broker rejects it: `Failed to fetch ACLs: { error_code: invalid_request [42] } (Invalid principal name: {*})`. The task keeps retrying every `interval` and failing every time, forever - and none of this shows up in `rpk shadow status` (`Security Migrator Task` just says `ACTIVE` / "has started" indefinitely). You only see it in the broker logs (`docker logs <destination> | grep -i security`).
2. There is **no wildcard/match-any for `principal`**. We tried `'*'` (rejected outright) and `'User:*'` (syntactically valid, but literal - it only matches ACLs granted to the exact principal string `"User:*"`, not "any user's ACL"). Proven by creating a brand-new principal's ACL under an active `User:*` filter - it never replicated. **One `acl_filters` entry only ever covers one exact principal.** To sync ACLs for multiple principals, list one entry per principal.
3. `resource_filter.pattern_type` must match the ACL's **own** pattern type exactly. A filter with `pattern_type: PREFIXED, name: demo-` will not pick up a `LITERAL`-pattern ACL on `demo-events`, even though the name obviously matches the prefix - it only catches ACLs that were themselves created with `--resource-pattern-type prefixed`. This is the more common way to get a silent "ACLs never sync" result, since most ad hoc ACLs (`rpk security acl create --topic X`) default to `LITERAL`.
4. Replication is **additive, not authoritative** - deleting the source ACL (or narrowing the filter so it no longer matches) does not retract the already-replicated copy on the destination. It has to be deleted there directly.

The corrected, verified-working example is in `config/shadow-link.yaml` now (`principal: 'User:demo-service-account'`, paired with an ACL actually created as `--resource-pattern-type prefixed`).

**Does the shadow link's own SASL config need to match a source-side user?** Yes, by construction - `client_options.authentication_configuration` (username/password/SCRAM mechanism) lives in the *same* config block as `bootstrap_servers`, which points at the *source* cluster. This is the credential the shadow link's internal client uses to authenticate its own pull connection to the source - it has to be a real account provisioned on the source with enough grants (read + describe on the shadowed topics/groups). It has nothing to do with any credential on the destination.

**Can an admin create separate SASL users on the destination for the replicated topic?** Yes, and it's the normal/expected way to grant access, since credentials never replicate. We created user `bob` directly on the destination with his own password and his own ACL on the shadow copy of `demo-events` - it worked immediately, coexisted with the (separately, correctly) replicated `alice` ACL with zero conflict, and had no effect on or from the shadow link. Destination-side users/ACLs are entirely independent of whatever exists on the source.

## Practical guidance for a real bidirectional setup

- Keep topic namespaces disjoint between the two directions (prefix-based
  filters like this demo's `demo-*` / `back-*`, or explicit topic lists).
  Don't use `'*'` on both sides unless you've deliberately reasoned about
  every topic name that could exist on both clusters.
- Before turning up a new direction, check for name collisions first:
  `rpk topic list` on both clusters, diff the names, resolve (rename/delete)
  before creating the link with a filter that would match them.
- Don't trust `STATE ACTIVE` alone. Check per-topic state + `LAG` in
  `rpk shadow status <link> -t` (or the dataplane topic endpoint in Cloud).
- `rpk shadow delete --force` fails over (promotes) all of that link's active
  shadow topics into regular writable topics before deleting the link. That
  promotion is exactly what creates the "now it exists natively on both
  sides" collision setup in Result 2 - worth remembering if you delete and
  recreate a link as part of testing or recovery.
- Schema Registry sync (`shadow_schema_registry_topic`, topic mode) shadows
  `_schemas` byte-for-byte and requires the source to be Redpanda. We did not
  enable it symmetrically in both directions in this test (only the forward
  link has it) specifically to avoid a guaranteed same-name collision on
  `_schemas` in both directions - that's a separate scenario worth testing
  explicitly before relying on it, not something this test set covers.
