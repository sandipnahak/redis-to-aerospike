# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This repository is a **reference example**; version bumps track **revision history**
for installs and issue reports, not a formal product release cadence.

## [Unreleased]

### Fixed

- Reading key expiry no longer fails on Redis-compatible sources that do not implement `PTTL` or `TTL` (Amazon MemoryDB Multi-Region returns `ERR unknown command` for both). `redis.ttl_command` selects `pttl`, `ttl`, `dump`, `expiretime`, or `pexpiretime`. `dump` reads an absolute expiry from a hash field inside the `DUMP` payload. Types whose read command is also missing, such as lists, are skipped instead of aborting the run.
- A write that sets a TTL is rejected with Aerospike error 22 (`AS_ERR_FORBIDDEN`) when the namespace has `nsup-period` 0. `ttl_command` of `dump`, `expiretime`, or `pexpiretime` now stops the run before those writes, because Redis `INFO` does not count expiries stored inside the value.

## [0.4.0] - 2026-06-29

### Added

- Shell tab completion for bash, zsh, and fish via `redis2aerospike --print-completion {bash,zsh,fish}` (powered by `shtab`).

## [0.3.0] - 2026-06-21

### Added

- Optional per-route **`hash_strategy`** and **`value_bin`** on `aerospike.set_routes`, with fallback to the global `hash_strategy` and `aerospike.value_bin`. Route `value_bin` is used only when the effective hash layout is **`map_bin`** (ignored for **`field_bins`**).
- Extended **`--set-route`** token forms: `PATTERN=SET=hash_strategy` and `PATTERN=SET=map_bin=CUSTOM_BIN` for a custom map bin name (fourth segment requires `map_bin` as the third).
- Migration preview lists per-route hash overrides when set.

### Changed

- Typing and static-analysis cleanup across the package (Redis standalone vs cluster client surfaces, migrator source/sink protocols, rate limiter and config annotations, and related Pyrefly/Pyright alignment).

## [0.2.0] - 2026-06-19

### Changed

- **Breaking:** PyPI distribution renamed from `redis-to-aerospike` to `redis2aerospike`; the installed CLI is now `redis2aerospike` (replacing `redis-to-aerospike`).

## [0.1.0] - 2026-06-18

### Added

- CLI tool `redis-to-aerospike` to migrate Redis (or Valkey / wire-compatible) data into Aerospike using native Aerospike types.
- Pluggable converters for Redis strings, hashes, lists, sets, and sorted sets, with TTL handling and unsupported-type skips.
- Multi-threaded producer/consumer pipeline (`SCAN` → bounded queue → workers).
- YAML, CLI, and environment-variable configuration; set routes for per-pattern Aerospike sets and key shaping.
- Redis standalone and Cluster, TLS/mTLS, ACL auth, URLs; Aerospike multi-host, Enterprise auth, TLS/mTLS, timeouts.
- Rate limits, batch writes, record-exists policies, hash strategies, TTL overflow policies, dry-run preview, and migration summary with exit codes.
