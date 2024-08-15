import logging
import time
from typing import Dict

from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.base import Extractor
from azure.identity import DefaultAzureCredential
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import DeltaError, TableNotFoundError
import pyarrow as pa
from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.metrics import Metrics
from cognite.client.data_classes import FileMetadataList
from datetime import datetime


class FileReplicator(Extractor):
    def __init__(self, metrics: Metrics, stop_event: CancellationToken) -> None:
        super().__init__(
            name="cdf_fabric_replicator_file",
            description="CDF Fabric Replicator",
            config_class=Config,
            metrics=metrics,
            use_default_state_store=False,
            version=__version__,
            cancellation_token=stop_event,
        )
        self.azure_credential = DefaultAzureCredential()
        self.stop_event = stop_event
        self.logger = logging.getLogger(self.name)

    def run(self) -> None:
        self.logger.info("Run Called for File Extractor...")
        # init/connect to destination
        self.state_store.initialize()

        self.logger.debug(f"Current File Config: {self.config.file}")

        if self.config.file is None:
            self.logger.warning("No File config found in config")
            return

        while not self.stop_event.is_set():
            start_time = time.time()  # Get the current time in seconds

            self.process_files()
            end_time = time.time()  # Get the time after function execution
            elapsed_time = end_time - start_time
            sleep_time = max(self.config.extractor.poll_time - elapsed_time, 0)

            if sleep_time > 0:
                self.logger.debug(f"Sleep for {sleep_time} seconds")
                self.stop_event.wait(sleep_time)

        self.logger.info("Stop event set. Exiting...")

    def process_files(self) -> None:
        for mime_type in self.config.file.mime_types:
            state_id = f"{mime_type}_state"
            last_updated_time = self.get_state(state_id)

            if last_updated_time is None:
                last_updated_time = 0
                self.logger.debug(
                    f"No last update time in state store with key {state_id}."
                )
            else:
                self.logger.debug(
                    f"Last updated time: {datetime.fromtimestamp(last_updated_time / 1000).isoformat()}"
                )

            feedRows = True
            self.logger.debug(f"Current File Config: {self.config.file}")

            while feedRows:
                files = self.cognite_client.files.list(
                    mime_type = mime_type,
                )
                if len(files) > 0:
                    try:
                        self.write_rows_to_lakehouse_table(
                            files, abfss_path=self.config.file.lakehouse_abfss_path_files
                        )
                        last_row = files[-1]
                        self.set_state(state_id, last_row.last_updated_time)
                    except DeltaError as e:
                        self.logger.error(
                            f"Error writing file rows to lakehouse tables: {e}"
                        )
                        raise e
                else:
                    feedRows = False

    def get_state(self, state_key: str) -> int | None:
        state = self.state_store.get_state(external_id=state_key)
        if isinstance(state, list):
            state = state[0]
        return int(state[0]) if state is not None and state[0] is not None else None

    def set_state(self, state_key: str, updated_time: int | None) -> None:
        if updated_time:
            self.state_store.set_state(external_id=state_key, low=updated_time)
            self.state_store.synchronize()
            self.logger.debug(f"State {state_key} set: {updated_time}")
        else:
            self.logger.debug(f"State {state_key} not set.")

    def write_rows_to_lakehouse_table(self, files: FileMetadataList, abfss_path: str) -> None:
        token = self.azure_credential.get_token("https://storage.azure.com/.default")

        files_dict = []
        for file in files:
            file_dict = dict()
            file_dict["external_id"] = str(file.external_id)
            file_dict["name"] = str(file.name)
            file_dict["source"] = str(file.source)
            file_dict["mime_type"] = str(file.mime_type)
            file_dict["metadata"] = str(file.metadata)
            file_dict["directory"] = str(file.directory)
            file_dict["asset_ids"] = str(file.asset_ids)
            file_dict["data_set_id"] = str(file.data_set_id)
            file_dict["labels"] = str(file.labels)
            file_dict["geo_location"] = str(file.geo_location)
            file_dict["source_created_time"] = str(file.source_created_time)
            file_dict["source_modified_time"] = str(file.source_modified_time)
            file_dict["security_categories"] = str(file.security_categories)
            file_dict["id"] = str(file.id)
            file_dict["uploaded"] = str(file.uploaded)
            file_dict["uploaded_time"] = str(file.uploaded_time)
            file_dict["created_time"] = str(file.created_time)
            file_dict["last_updated_time"] = str(file.last_updated_time)
            files_dict.append(file_dict)
        if len(files_dict) > 0:
            self.logger.info(f"Writing {len(files)} rows to '{abfss_path}' table...")
            data = pa.Table.from_pylist(files_dict)
            storage_options = {
                "bearer_token": token.token,
                "use_fabric_endpoint": "true",
            }

            try:
                self.write_or_merge_to_lakehouse_table(
                    abfss_path, storage_options, data
                )
            except DeltaError as e:
                self.logger.error(f"Error writing rows to lakehouse tables: {e}")
                raise e

            self.logger.info("Done.")

    def write_or_merge_to_lakehouse_table(
        self, abfss_path: str, storage_options: Dict[str, str], data: pa.Table
    ) -> None:
        try:
            dt = DeltaTable(
                abfss_path,
                storage_options=storage_options,
            )

            (
                dt.merge(
                    source=data,
                    predicate="s.key = t.key",
                    source_alias="s",
                    target_alias="t",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        except TableNotFoundError:
            write_deltalake(
                abfss_path,
                data,
                mode="append",
                engine="rust",
                schema_mode="merge",
                storage_options=storage_options,
            )
