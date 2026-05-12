# Security policy

## Reporting a vulnerability

Email <zachary@ambroslabs.io> with the details. Please do not file
public GitHub issues for security reports.

We aim to acknowledge new reports within two business days and to
patch confirmed vulnerabilities in coordination with the reporter
before any public disclosure.

## In scope

- Anything in this repository.
- The on-disk artifacts malcom produces (snapshot dirs, application.db,
  bootstrap output).

## Out of scope

- Bugs in cosmos-sdk, cometbft, pebble, or other upstream
  dependencies. Report those to the relevant project.
- Denial-of-service requiring an attacker-controlled snapshot served
  via the standard cometbft state-sync protocol — see
  [THREAT-MODEL.md](THREAT-MODEL.md) for the trust boundary; malcom
  treats unverified P2P snapshots as adversarial input and the
  defenses live in `malcom snapshot fetch`'s per-chunk hashing and
  in `malcom verify`.
