# Changelog

## 1.1.4
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074
  * Update singer-python to use `should_sync_field` for field selection [#18](https://github.com/singer-io/tap-exacttarget/pull/18)

## 1.1.3
  * Fixes a bug where the data extension replicator uses the wrong time unit and misses data [#16](https://github.com/singer-io/tap-exacttarget/pull/16)

## 1.1.2
  * Convert boolean data types to actual bools [#15](https://github.com/singer-io/tap-exacttarget/pull/15)

## 1.1.1
  * Apply the timeout value to the underlying soap Transport object [#14](https://github.com/singer-io/tap-exacttarget/pull/14)

## 1.1.0
  * Make all fields nullable [#11](https://github.com/singer-io/tap-exacttarget/pull/11)

## 1.0.4
  * Skip empty subscriber batches [#9](https://github.com/singer-io/tap-exacttarget/pull/9)
