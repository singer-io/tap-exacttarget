"""Mocked integration tests package for tap-exacttarget.

Unlike tests/*.py (tap_tester-based tests that require live
ExactTarget/Marketing Cloud credentials) and tests/unittests/*.py (unit
tests that mock at the HTTP/SOAP transport layer), the tests in this
package exercise the tap's real discover() and sync() entry points
end-to-end against a fully mocked Client stand-in. This gives fast,
credential-free coverage of discovery/catalog metadata, field selection,
bookmarking, and start_date handling.
"""
