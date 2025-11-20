import time
import json
import requests
from typing import Dict, List, Optional
from singer import Transformer, get_logger, write_record
from singer.transform import SchemaMismatch
from singer.utils import now
import dateutil.parser
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream, fixed_cst, strptime_to_cst

LOGGER = get_logger()


class JourneyHistory(IncrementalStream):
    """Class for Journey History stream.
    
    Exports contact-level journey interaction data including:
    - Contact identifiers (ContactKey, ContactID)
    - Journey activity data (ActivityID, ActivityName, ActivityType)
    - Event timestamps (EventDate)
    - Journey metadata (JourneyID, JourneyName, VersionNumber)
    - Activity outcomes and details
    - Custom attributes and data
    """

    client: Client

    stream = "journey_history"
    tap_stream_id = "journey_history"
    object_ref = "JourneyHistory"
    key_properties = ["ContactKey", "JourneyID", "VersionNumber", "ActivityID", "EventDate"]
    replication_key = "EventDate"
    valid_replication_keys = ["EventDate"]
    config_start_key = "start_date"
    
    interactions_endpoint = "interaction/v1/interactions"

    def get_all_journeys(self) -> List[Dict]:
        """Retrieve all available journeys with optional API-level filtering.

        Applies status filtering directly in the API call for better performance.
        Default includes all statuses for comprehensive data collection.
        """
        endpoint = self.interactions_endpoint
        headers = {"Authorization": f"Bearer {self.client.access_token}"}
        final_url = f"{self.client.rest_url}{endpoint}"
        
        # Get filter configuration - default to all statuses for comprehensive sync
        include_statuses = self.client.config.get(
            "journey_statuses",
            None  # None means fetch all statuses
        )

        all_journeys = []
        page = 1
        page_size = 100
        
        while True:
            params = {
                "$page": page,
                "$pageSize": page_size,
                "$orderBy": "modifiedDate DESC"
            }
            
            # Add status filter if configured
            # If None, fetch all statuses for comprehensive data collection
            if include_statuses:
                # OData filter syntax: status eq 'Running' or status eq 'Stopped' ...
                status_filters = " or ".join([f"status eq '{status}'" for status in include_statuses])
                params["$filter"] = status_filters
                LOGGER.info("Fetching journeys page %d with status filter: %s", page, include_statuses)
            else:
                LOGGER.info("Fetching journeys page %d (all statuses)", page)
            
            try:
                response = requests.get(
                    final_url, 
                    headers=headers, 
                    params=params,
                    timeout=self.client.timeout
                )
                response.raise_for_status()
                
                data = response.json()
                items = data.get("items", [])
                
                if not items:
                    break
                
                all_journeys.extend(items)
                
                count = data.get("count", 0)
                if len(all_journeys) >= count or len(items) < page_size:
                    break
                
                page += 1
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    LOGGER.warning("No journeys available")
                    break
                raise
        
        LOGGER.info("Found %d total journeys", len(all_journeys))

        # Log status distribution for visibility
        status_counts = {}
        for journey in all_journeys:
            status = journey.get("status", "Unknown")
            status_counts[status] = status_counts.get(status, 0) + 1

        LOGGER.info("Journey status distribution: %s", status_counts)

        return all_journeys

    def filter_journeys_for_sync(self, journeys: List[Dict]) -> List[Dict]:
        """Filter journeys for specific IDs and version selection.

        Handles:
        - Specific journey ID inclusion/exclusion (by ID or key)
        - Latest version selection per journey

        Status filtering is now handled at API level in get_all_journeys()
        """
        exclude_journey_ids = self.client.config.get("exclude_journey_ids", [])
        include_journey_ids = self.client.config.get("include_journey_ids", None)
        include_all_versions = self.client.config.get("journey_include_all_versions", False)
        
        filtered = []
        journey_map = {}
        
        for journey in journeys:
            journey_id = journey.get("id")
            journey_key = journey.get("key")
            journey_version = journey.get("version", 1)
            journey_status = journey.get("status")
            journey_name = journey.get("name", "Unknown")
            
            if not journey_id:
                continue
            
            # If specific IDs configured, only use those
            if include_journey_ids is not None:
                if journey_id not in include_journey_ids and journey_key not in include_journey_ids:
                    continue
            
            # Skip excluded journeys (by ID or key)
            if journey_id in exclude_journey_ids or journey_key in exclude_journey_ids:
                LOGGER.info("Excluding journey %s (%s)", journey_id, journey_name)
                continue
            
            # Handle versioning
            if include_all_versions:
                # Include every version
                filtered.append({
                    "id": journey_id,
                    "key": journey_key,
                    "name": journey_name,
                    "version": journey_version,
                    "status": journey_status
                })
                LOGGER.info(
                    "Including journey %s (%s) v%s - status: %s",
                    journey_id, journey_name, journey_version, journey_status
                )
            else:
                # Track only latest version per journey
                if journey_id not in journey_map or journey_version > journey_map[journey_id]["version"]:
                    journey_map[journey_id] = {
                        "id": journey_id,
                        "key": journey_key,
                        "name": journey_name,
                        "version": journey_version,
                        "status": journey_status
                    }
        
        # Convert map to list if not including all versions
        if not include_all_versions:
            filtered = list(journey_map.values())
            for j in filtered:
                LOGGER.info(
                    "Including journey %s (%s) v%s (latest) - status: %s",
                    j["id"], j["name"], j["version"], j["status"]
                )
        
        LOGGER.info("Filtered to %d journey versions for sync", len(filtered))
        return filtered

    def request_export(self, journey_id: str, version: int, start_date: str, end_date: str) -> Optional[str]:
        """Request a journey history export.
        
        Returns:
            request_id for polling, or None if no data available
        """
        endpoint = f"{self.interactions_endpoint}/journeyhistory/{journey_id}/export"
        
        payload = {
            "startDate": start_date,
            "endDate": end_date,
            "version": version
        }
        
        headers = {
            "Authorization": f"Bearer {self.client.access_token}",
            "Content-Type": "application/json"
        }
        final_url = f"{self.client.rest_url}{endpoint}"
        
        try:
            response = requests.post(
                final_url, 
                headers=headers, 
                json=payload, 
                timeout=self.client.timeout
            )
            
            if response.status_code == 204:
                LOGGER.info(
                    "No history data for journey %s v%s in date range %s to %s",
                    journey_id, version, start_date, end_date
                )
                return None
            
            response.raise_for_status()
            data = response.json()
            return data.get("requestId")
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                LOGGER.warning("Journey %s v%s not found", journey_id, version)
                return None
            raise

    def check_export_status(self, journey_id: str, request_id: str) -> Dict:
        """Check export request status."""
        endpoint = f"{self.interactions_endpoint}/journeyhistory/{journey_id}/export/{request_id}"
        
        headers = {"Authorization": f"Bearer {self.client.access_token}"}
        final_url = f"{self.client.rest_url}{endpoint}"
        
        response = requests.get(final_url, headers=headers, timeout=self.client.timeout)
        response.raise_for_status()
        
        return response.json()

    def download_export(self, journey_id: str, request_id: str) -> list:
        """Download journey history export.
        
        The export contains newline-delimited JSON with fields like:
        - ContactKey: Subscriber/contact key
        - ContactID: Contact ID
        - JourneyID: Journey identifier
        - JourneyName: Name of the journey
        - VersionNumber: Journey version
        - ActivityID: Activity identifier
        - ActivityName: Name of the activity
        - ActivityType: Type (e.g., EMAIL, SMS, DECISION_SPLIT, etc.)
        - ActivityExternalKey: External key for activity
        - EventDate: When the event occurred
        - ActivityInstanceID: Instance of the activity
        - TransactionTime: Transaction timestamp
        - IsTest: Whether this was a test send
        - Additional custom fields and attributes
        """
        endpoint = f"{self.interactions_endpoint}/journeyhistory/{journey_id}/export/{request_id}/download"
        
        headers = {"Authorization": f"Bearer {self.client.access_token}"}
        final_url = f"{self.client.rest_url}{endpoint}"
        
        response = requests.get(final_url, headers=headers, timeout=self.client.timeout)
        response.raise_for_status()
        
        # Parse newline-delimited JSON
        records = []
        for line in response.text.strip().split('\n'):
            if line.strip():
                try:
                    record = json.loads(line)
                    # All fields from the API are preserved
                    records.append(record)
                except json.JSONDecodeError as e:
                    LOGGER.warning("Failed to parse JSON line: %s", str(e))
                    continue
        
        return records

    def wait_for_export(self, journey_id: str, request_id: str, max_wait: int = 900) -> bool:
        """Poll until export is ready or timeout."""
        elapsed = 0
        wait_interval = 5
        
        while elapsed < max_wait:
            status = self.check_export_status(journey_id, request_id)
            state = status.get("state")
            
            if state == "Complete":
                LOGGER.info("Export complete for journey %s, request %s", journey_id, request_id)
                return True
            elif state == "Error":
                error_msg = status.get("errorMessage", "Unknown error")
                LOGGER.error("Export failed for journey %s: %s", journey_id, error_msg)
                return False
            
            LOGGER.debug(
                "Export status for journey %s: %s (waited %ds)", 
                journey_id, state, elapsed
            )
            
            time.sleep(wait_interval)
            elapsed += wait_interval
            wait_interval = min(wait_interval * 1.2, 30)
        
        LOGGER.warning("Export timed out for journey %s after %d seconds", journey_id, max_wait)
        return False

    def get_records(self, start_date, stream_metadata: Dict, schema: Dict):
        """Get journey history records for all journeys."""
        
        all_journeys = self.get_all_journeys()
        journeys_to_sync = self.filter_journeys_for_sync(all_journeys)
        
        if not journeys_to_sync:
            LOGGER.warning("No journeys found to sync")
            return
        
        for start_dt, end_dt in self.create_date_windows(
            start_date, 
            now().astimezone(tz=fixed_cst), 
            self.client.date_window
        ):
            start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
            end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
            
            LOGGER.info(
                "Fetching journey history from %s to %s for %d journeys",
                start_str, end_str, len(journeys_to_sync)
            )
            
            for journey_meta in journeys_to_sync:
                journey_id = journey_meta["id"]
                journey_version = journey_meta["version"]
                journey_name = journey_meta["name"]
                
                try:
                    LOGGER.info("Processing journey %s (%s) v%s", journey_id, journey_name, journey_version)
                    
                    request_id = self.request_export(journey_id, journey_version, start_str, end_str)
                    
                    if not request_id:
                        continue
                    
                    LOGGER.info("Export requested: request_id=%s", request_id)
                    
                    if self.wait_for_export(journey_id, request_id):
                        records = self.download_export(journey_id, request_id)
                        LOGGER.info("Downloaded %d records for journey %s", len(records), journey_name)
                        yield from records
                    else:
                        LOGGER.error("Failed to complete export for journey %s", journey_id)
                        
                except Exception as ex:
                    LOGGER.error("Error processing journey %s: %s", journey_id, str(ex), exc_info=True)
                    continue

    def sync(
        self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer
    ) -> Dict:
        """Sync journey history stream."""
        
        current_max_bookmark_date = bookmark_date_utc = strptime_to_cst(self.get_bookmark(state))
        records_processed = 0
        schema_mismatch_count = 0
        
        for record in self.get_records(bookmark_date_utc, stream_metadata, schema):
            try:
                if record.get(self.replication_key):
                    record_date = strptime_to_cst(record[self.replication_key])
                    if record_date > current_max_bookmark_date:
                        current_max_bookmark_date = record_date
                
                transformed_record = transformer.transform(record, schema, stream_metadata)
                write_record(self.tap_stream_id, transformed_record)
                records_processed += 1
                
                if records_processed % 1000 == 0:
                    LOGGER.info("Processed %d journey history records", records_processed)
                
            except SchemaMismatch as ex:
                schema_mismatch_count += 1
                LOGGER.warning("Schema mismatch: %s", str(ex))
            except Exception as ex:
                LOGGER.error("Error processing record: %s", str(ex))
                continue
        
        LOGGER.info(
            "Stream sync complete: %d records, %d schema mismatches",
            records_processed, schema_mismatch_count
        )
        
        state = self.write_bookmark(
            state, 
            value=current_max_bookmark_date.isoformat(timespec='microseconds')
        )
        return state