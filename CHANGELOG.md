# Changelog

## [0.3.2a13] - 2026-07-23

- Bound a handler lifecycle after accepting an uncorrelated reply through its
  unique admitted client or site scope, preventing the listener worker from
  remaining occupied until the full query timeout when OVOS also omits
  correlation from the handler-complete event.

## [0.3.2a12] - 2026-07-23

- Index active query callbacks by explicit query and admitted client scope so
  each runtime-bus lifecycle event reaches only its matching query instead of
  producing quadratic listener work under concurrent traffic.

## [0.3.2a11] - 2026-07-22

- Keep runtime-bus event subscriptions immutable while queries are active and
  dispatch responses through an agent-owned registry, preventing pyee emitter
  deadlocks under sustained query traffic.

## [0.3.2a10] - 2026-07-22

- Require the managed runtime's post-transform query receipt before treating
  delivery as confirmed, while preserving exact retry and one shared deadline.

## [0.3.2a9] - 2026-07-22

- Use the idempotent query-reservation receipt as the runtime application
  liveness proof and share one absolute delivery deadline across reservation,
  acceptance, and the complete query lifecycle.

## [0.3.2a8] - 2026-07-22

- Keep queries open through the authoritative OVOS skill-handler lifecycle so
  delayed final replies cannot bleed into the next query on the same client.

## [0.3.2a3](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/tree/0.3.2a3) (2026-07-04)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/compare/0.3.2a2...0.3.2a3)

**Merged pull requests:**

- Update dependency pyee to v13 [\#1](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/pull/1) ([renovate[bot]](https://github.com/apps/renovate))

## [0.3.2a2](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/tree/0.3.2a2) (2026-07-04)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/compare/0.3.2a1...0.3.2a2)

**Merged pull requests:**

- test: hivescope e2e + CI [\#19](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/pull/19) ([JarbasAl](https://github.com/JarbasAl))

## [0.3.2a1](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/tree/0.3.2a1) (2026-06-22)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/compare/0.3.1a1...0.3.2a1)

**Merged pull requests:**

- fix: fail fast when the OVOS messagebus is unreachable [\#17](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/pull/17) ([JarbasAl](https://github.com/JarbasAl))

## [0.3.1a1](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/tree/0.3.1a1) (2026-06-06)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/compare/0.3.0a4...0.3.1a1)

**Merged pull requests:**

- fix\(deps\): require ovos-bus-client\>=2.0.0a3 [\#15](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/pull/15) ([JarbasAl](https://github.com/JarbasAl))

## [0.3.0a4](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/tree/0.3.0a4) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/compare/0.3.0a3...0.3.0a4)

**Merged pull requests:**

- ci: fix integration workflow startup\_failure \(system\_deps input\) [\#13](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/pull/13) ([JarbasAl](https://github.com/JarbasAl))

## [0.3.0a3](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/tree/0.3.0a3) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/compare/0.3.0a2...0.3.0a3)

**Merged pull requests:**

- test: live-OVOS e2e via ovoscope [\#11](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/pull/11) ([JarbasAl](https://github.com/JarbasAl))

## [0.3.0a2](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/tree/0.3.0a2) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/compare/0.3.0a1...0.3.0a2)

**Merged pull requests:**

- docs: zero-to-hero README and docs/ [\#9](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/pull/9) ([JarbasAl](https://github.com/JarbasAl))

## [0.3.0a1](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/tree/0.3.0a1) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/compare/0.2.0a2...0.3.0a1)

**Merged pull requests:**

- feat: natural\_language\_query companion [\#7](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/pull/7) ([JarbasAl](https://github.com/JarbasAl))

## [0.2.0a2](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/tree/0.2.0a2) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/compare/0.2.0a1...0.2.0a2)

**Merged pull requests:**

- ci: drop hivemind-core branch pin \(→ 4.3.0a2\) [\#5](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/pull/5) ([JarbasAl](https://github.com/JarbasAl))

## [0.2.0a1](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/tree/0.2.0a1) (2026-06-04)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/compare/0.1.0...0.2.0a1)

**Merged pull requests:**

- feat\(policy\): OVOSAgentPolicy + OVOS-specific mutations [\#3](https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin/pull/3) ([JarbasAl](https://github.com/JarbasAl))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
