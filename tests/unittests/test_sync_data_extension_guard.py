"""Tests for the Data Extension discovery guard in sync.py.

These cover the fix for the bug where sync() unconditionally called
discover_dao_streams() before inspecting the selected catalog, so a Data
Extension permission failure would abort every sync even when no Data
Extension streams were selected.
"""
import importlib
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

from tap_exacttarget.discover_dataextensionobj import DATA_EXTENSION_STREAM_PREFIX
from tap_exacttarget.exceptions import MarketingCloudPermissionFailure

sync_module = importlib.import_module("tap_exacttarget.sync")


class CatalogMock:
    def __init__(self, selected_items):
        self._selected_items = selected_items

    def get_selected_streams(self, _state):
        return self._selected_items


def _stream_item(tap_stream_id):
    schema = SimpleNamespace(to_dict=lambda: {"properties": {}})
    return SimpleNamespace(
        tap_stream_id=tap_stream_id,
        schema=schema,
        metadata=[],
        replication_key=None,
    )


class TestDiscoverDaoStreamsIfSelected(TestCase):
    def test_skips_discovery_when_no_data_extension_stream_selected(self):
        """discover_dao_streams() must not be called at all when no selected
        stream is a Data Extension stream."""
        client = MagicMock()
        with patch.object(sync_module, "discover_dao_streams") as mock_discover:
            doa_streams, failed_streams = sync_module._discover_dao_streams_if_selected(
                client, ["campaigns", "email"]
            )

        mock_discover.assert_not_called()
        self.assertEqual(doa_streams, {})
        self.assertEqual(failed_streams, [])

    def test_calls_discovery_when_data_extension_stream_selected(self):
        """discover_dao_streams() must be called when a data_extension_*
        stream is among the selected streams, and its result returned."""
        client = MagicMock()
        expected_streams = {f"{DATA_EXTENSION_STREAM_PREFIX}foo": Mock()}
        with patch.object(sync_module, "discover_dao_streams", return_value=expected_streams) as mock_discover:
            doa_streams, failed_streams = sync_module._discover_dao_streams_if_selected(
                client, ["campaigns", f"{DATA_EXTENSION_STREAM_PREFIX}foo"]
            )

        mock_discover.assert_called_once_with(client=client)
        self.assertEqual(doa_streams, expected_streams)
        self.assertEqual(failed_streams, [])

    def test_permission_failure_only_fails_selected_data_extension_streams(self):
        """When discover_dao_streams() raises MarketingCloudPermissionFailure,
        only the selected Data Extension stream(s) should be recorded as
        failed; the caller gets an empty doa_streams dict rather than a
        propagated exception."""
        client = MagicMock()
        err = MarketingCloudPermissionFailure("no access")
        with patch.object(sync_module, "discover_dao_streams", side_effect=err) as mock_discover:
            doa_streams, failed_streams = sync_module._discover_dao_streams_if_selected(
                client, ["campaigns", f"{DATA_EXTENSION_STREAM_PREFIX}foo"]
            )

        mock_discover.assert_called_once_with(client=client)
        self.assertEqual(doa_streams, {})
        self.assertEqual(len(failed_streams), 1)
        self.assertEqual(failed_streams[0][0], f"{DATA_EXTENSION_STREAM_PREFIX}foo")
        self.assertEqual(failed_streams[0][1], err)


class TestSyncDataExtensionPermissionHandling(TestCase):
    @patch("tap_exacttarget.sync.singer.write_schema")
    @patch("tap_exacttarget.sync.singer.write_state")
    @patch("tap_exacttarget.sync.singer.set_currently_syncing")
    @patch("tap_exacttarget.sync.singer.metadata.to_map", side_effect=lambda metadata: metadata)
    @patch("tap_exacttarget.sync.singer.Transformer")
    def test_sync_syncs_remaining_streams_when_data_extension_access_denied(
        self, mock_transformer_cls, mock_to_map, mock_set_currently_syncing, mock_write_state, mock_write_schema
    ):
        """Core regression test: a Data Extension permission failure must not
        abort syncing of other, accessible streams."""
        state = {"bookmarks": {}}
        dao_stream_id = f"{DATA_EXTENSION_STREAM_PREFIX}foo"
        campaigns_item = _stream_item("campaigns")
        dao_item = _stream_item(dao_stream_id)

        catalog = CatalogMock([campaigns_item, dao_item])

        campaigns_stream = MagicMock(key_properties=["id"], replication_key=None, tap_stream_id="campaigns")
        mock_transformer = MagicMock()
        mock_transformer.__enter__.return_value = mock_transformer
        mock_transformer_cls.return_value = mock_transformer
        mock_set_currently_syncing.side_effect = lambda current_state, syncing: current_state

        err = MarketingCloudPermissionFailure("no access to Data Extensions")

        with patch.object(sync_module, "discover_dao_streams", side_effect=err) as mock_discover:
            with patch.dict(sync_module.STREAMS, {"campaigns": Mock(return_value=campaigns_stream)}, clear=False):
                sync_module.sync(MagicMock(), catalog, state)

        mock_discover.assert_called_once()
        # The accessible stream was still synced.
        campaigns_stream.sync.assert_called_once()
        mock_write_schema.assert_any_call("campaigns", {"properties": {}}, ["id"], None)

    @patch("tap_exacttarget.sync.singer.write_schema")
    @patch("tap_exacttarget.sync.singer.write_state")
    @patch("tap_exacttarget.sync.singer.set_currently_syncing")
    @patch("tap_exacttarget.sync.singer.metadata.to_map", side_effect=lambda metadata: metadata)
    @patch("tap_exacttarget.sync.singer.Transformer")
    def test_sync_does_not_call_discover_dao_streams_when_not_selected(
        self, mock_transformer_cls, mock_to_map, mock_set_currently_syncing, mock_write_state, mock_write_schema
    ):
        """discover_dao_streams() should be skipped entirely (no SOAP/API
        call) when no Data Extension stream is selected, even if the
        credentials would otherwise fail that call."""
        state = {"bookmarks": {}}
        campaigns_item = _stream_item("campaigns")
        catalog = CatalogMock([campaigns_item])

        campaigns_stream = MagicMock(key_properties=["id"], replication_key=None, tap_stream_id="campaigns")
        mock_transformer = MagicMock()
        mock_transformer.__enter__.return_value = mock_transformer
        mock_transformer_cls.return_value = mock_transformer
        mock_set_currently_syncing.side_effect = lambda current_state, syncing: current_state

        with patch.object(sync_module, "discover_dao_streams") as mock_discover:
            with patch.dict(sync_module.STREAMS, {"campaigns": Mock(return_value=campaigns_stream)}, clear=False):
                sync_module.sync(MagicMock(), catalog, state)

        mock_discover.assert_not_called()
        campaigns_stream.sync.assert_called_once()

    @patch("tap_exacttarget.sync.singer.write_schema")
    @patch("tap_exacttarget.sync.singer.write_state")
    @patch("tap_exacttarget.sync.singer.set_currently_syncing")
    @patch("tap_exacttarget.sync.singer.metadata.to_map", side_effect=lambda metadata: metadata)
    @patch("tap_exacttarget.sync.singer.Transformer")
    def test_sync_skips_stream_missing_from_streams_registry_without_keyerror(
        self, mock_transformer_cls, mock_to_map, mock_set_currently_syncing, mock_write_state, mock_write_schema
    ):
        """If a selected stream's tap_stream_id ends up missing from STREAMS
        (e.g. a Data Extension stream selected in a stale catalog whose
        access has since been revoked), sync() must skip it gracefully
        instead of raising KeyError."""
        state = {"bookmarks": {}}
        dao_stream_id = f"{DATA_EXTENSION_STREAM_PREFIX}foo"
        dao_item = _stream_item(dao_stream_id)
        catalog = CatalogMock([dao_item])

        mock_set_currently_syncing.side_effect = lambda current_state, syncing: current_state
        mock_transformer = MagicMock()
        mock_transformer.__enter__.return_value = mock_transformer
        mock_transformer_cls.return_value = mock_transformer

        err = MarketingCloudPermissionFailure("no access")
        with patch.object(sync_module, "discover_dao_streams", side_effect=err):
            with patch.dict(sync_module.STREAMS, {}, clear=False):
                if dao_stream_id in sync_module.STREAMS:
                    del sync_module.STREAMS[dao_stream_id]
                # Should not raise KeyError.
                sync_module.sync(MagicMock(), catalog, state)

        mock_write_schema.assert_not_called()
