# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **BREAKING CHANGE**: Default port changed from `9221` to `7379` for Redis compatibility
  - The configuration system has been migrated to Redis-style format (similar to `redis.conf`)
  - Configuration file moved from INI format to Redis-style format at `src/conf/kiwi.conf`
  - Default port is now `7379` (a Redis-compatible variant of the standard `6379` port)

### Migration Guide

If you are upgrading from a previous version that used port `9221`:

1. **Update client connections**: Change any client applications connecting to Kiwi from port `9221` to port `7379`

2. **Update firewall rules**: If you have firewall rules allowing port `9221`, update them to allow port `7379`

3. **Configuration file migration**: The configuration format has changed from INI to Redis-style:
   - Old format: `config.ini` with `port=9221`
   - New format: `kiwi.conf` with `port 7379`

4. **Custom port configuration**: If you need to use a different port, update the `port` setting in `src/conf/kiwi.conf`:
   ```conf
   # Accept connections on the specified port, default is 7379.
   # port 7379
   port YOUR_CUSTOM_PORT
   ```

5. **Docker/Container deployments**: Update any port mappings from `9221` to `7379`

**Note**: Port `7379` was chosen to indicate Redis compatibility while avoiding conflicts with the standard Redis port (`6379`).

### Added

- Redis-style configuration file format support
- Inline comment support in configuration files (using `#`)
- Configuration file parsing with better error messages

### Fixed

- Port number consistency across codebase
- Configuration default values now properly aligned
