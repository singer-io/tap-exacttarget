# Changelog

## 1.7.0
  * Add retry logic to requests [#60] (https://github.com/singer-io/tap-exacttarget/pull/60)
  * Print user friendly error messages [#65] (https://github.com/singer-io/tap-exacttarget/pull/65)
  * Check best practices [#66] (https://github.com/singer-io/tap-exacttarget/pull/66)
  * Keys should be marked automatic and list_sends stream does not bookmark correctly [#68] (https://github.com/singer-io/tap-exacttarget/pull/68)
  * Respect field selection [#67] (https://github.com/singer-io/tap-exacttarget/pull/67)
  * Respect the start date for all streams [#63] (https://github.com/singer-io/tap-exacttarget/pull/63)
  * Make batch_size apply to the first page and TDL-14895: Use the pagination__list_subscriber_interval_quantity config value correctly [#64] (https://github.com/singer-io/tap-exacttarget/pull/64)
  * Separation of Schema from .Py [#60] (https://github.com/singer-io/tap-exacttarget/pull/60

## 1.6.1
  * Add OAuth2 support but using server-to-server integrations [#47](https://github.com/singer-io/tap-exacttarget/pull/47)

## 1.6.0
  * Add OAuth2 support [#43](https://github.com/singer-io/tap-exacttarget/pull/43)

## 1.5.1
  * Don't add duplicate metadata to catalog during discovery mode [#38](https://github.com/singer-io/tap-exacttarget/pull/38)

## 1.5.0
  * Update Singer patterns to use metadata for stream selection [#37](https://github.com/singer-io/tap-exacttarget/pull/37)

## 1.4.0
  * Adds a configurable batch_size to reduce the data read from the SFMC API and limit memory usage [#36](https://github.com/singer-io/tap-exacttarget/pull/36)

## 1.1.6
  * Adds support for using tenant subdomains for authentication [#23](https://github.com/singer-io/tap-exacttarget/pull/23)
  * Removes local copy of FuelSDK in favor of PyPi package.
  * Removes unused dependencies

## 1.1.5
  * Adds `URL` to the `event` stream for click events [#19](https://github.com/singer-io/tap-exacttarget/pull/19)

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
