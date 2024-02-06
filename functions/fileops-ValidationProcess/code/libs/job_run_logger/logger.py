"""Classes for Job Run Logger"""
import json

from typing import List, Optional
from dataclasses import dataclass

from .enums import TriggerTypes
from .data_classes import FileValidation, ColumnValidation, DataValidation, SchemaValidation


@dataclass
class JobRunLogger:
    """Dataclass representing job run logging information

           Variables:
                business_process_id (str | None): Identifier for the business process
                business_process_name (str | None): Name of the business process
                job_id (str | None): Job Id
                job_name (str | None): Name of the job
                job_run_id (str | None): Identifier for the job run
                file_name (str | None): Name of the file which is being processed
                started_time (str | None): Timestamp when the job run started
                completed_time (str | None): Timestamp when the job run completed
                trigger_type (TriggerTypes | None): Type of trigger for the job run
                trigger_by (str | None): Entity that triggered the job run (e.g., FileCentral/Username)
                job_status (str | None): Status of the job (e.g., in progress, completed)
                job_validation_status (str | None): Validation status of the job
                file_validations (List[FileValidation] | None): List of file validations
                column_validations (List[ColumnValidation] | None): List of column validations
                data_validations (List[DataValidation] | None): List of data validations
        """

    def __init__(self,
                 business_process_id: Optional[str] = None,
                 business_process_name: Optional[str] = None,
                 job_name: Optional[str] = None,
                 job_id: Optional[str] = None,
                 job_run_id: Optional[str] = None,
                 file_name: Optional[str] = None,
                 file_path: Optional[str] = None,
                 bucket_name: Optional[str] = None,
                 started_time: Optional[str] = None,
                 completed_time: Optional[str] = None,
                 trigger_type: Optional[TriggerTypes] = TriggerTypes.MANUAL,
                 trigger_by: Optional[str] = None,
                 job_status: Optional[str] = None,
                 job_validation_status: Optional[str] = None,
                 file_validations: Optional[List[FileValidation]] = None,
                 column_validations: Optional[List[ColumnValidation]] = None,
                 data_validations: Optional[List[DataValidation]] = None,
                 schema_validations: Optional[List[SchemaValidation]] = None):
        """JobRunLogger initializator

                    Args:
                        file_path (str): Folder path
                        file_name (str): File name

                    Returns:
                        str: The contents of the file
                        """
        self.business_process_id = business_process_id
        self.business_process_name = business_process_name
        self.job_id = job_id
        self.job_name = job_name
        self.job_run_id = job_run_id
        self.file_name = file_name
        self.file_path = file_path
        self.bucket_name = bucket_name
        self.started_time = started_time
        self.completed_time = completed_time
        self.trigger_type = trigger_type
        self.trigger_by = trigger_by
        self.job_status = job_status
        self.job_validation_status = job_validation_status
        self.file_validations = file_validations
        self.column_validations = column_validations
        self.data_validations = data_validations
        self.schema_validations = schema_validations

    def to_dict(self):
        """Converts a JobRunLogger instance to a JSON-compatible dictionary"""
        file_validations = [{
            "event": file_validation.event.value,
            "file_location": file_validation.file_location,
            "result": file_validation.result.value,
            "timestamp": file_validation.timestamp
        }
            for file_validation in self.file_validations] if self.file_validations else None

        column_validations = [{
            "event": column_validation.event.value,
            "columns": column_validation.columns,
            "result": column_validation.result.value,
            "timestamp": column_validation.timestamp
        }
            for column_validation in self.column_validations] if self.column_validations else None

        data_validations = [{
            "event": data_validation.event.value,
            "column": data_validation.column,
            "rows": data_validation.rows,
            "result": data_validation.result.value,
            "timestamp": data_validation.timestamp
        }
            for data_validation in self.data_validations] if self.data_validations else None

        schema_validations = [{"event": schema_validation.event.value if schema_validation.event is not None else None,
                               "value": schema_validation.value if schema_validation.value is not None else None,
                               "result": schema_validation.result.value if schema_validation.result is not None else None}
                              for schema_validation in self.schema_validations] if self.schema_validations else None

        json_data = {
            "business_process_id": self.business_process_id,
            "business_process_name": self.business_process_name,
            "job_id": self.job_id,
            "job_run_id": self.job_run_id,
            "job_name": self.job_name,
            "file_name": self.file_name,
            "file_path": self.file_path,
            "bucket_name": self.bucket_name,
            "started_time": self.started_time,
            "completed_time": self.completed_time,
            "trigger_type": self.trigger_type.value,  # using the trigger type value
            "trigger_by": self.trigger_by,
            "job_status": self.job_status,
            "job_validation_status": self.job_validation_status,
            "file_validations": file_validations,
            "column_validations": column_validations,
            "data_validations": data_validations,
            "schema_validations": schema_validations
        }

        return json_data

    def to_json(self):
        """Converts a JobRunLogger instance to JSON"""
        json_data = self.to_dict()
        return json.dumps(json_data)
