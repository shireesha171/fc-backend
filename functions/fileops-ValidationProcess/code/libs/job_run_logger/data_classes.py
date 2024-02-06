from typing import List, Optional
from dataclasses import dataclass

from .enums import FileValidationTypes, ValidationResultTypes, ColumnValidationTypes, DataValidationTypes, \
    SchemaValidationTypes


@dataclass
class FileError:
    """Dataclass representing possible errors in a file

            Variables:
                not_found (str | None): Error for file not found
                file_format (str | None): Error for file format
                file_size (str | None): Error for file size
                delimiter (str | None): Error for incorrect delimiter
                encoding (str | None): Error for encoding issues
        """

    not_found: Optional[str]
    file_format: Optional[str]
    file_size: Optional[str]
    delimiter: Optional[str]
    encoding: Optional[str]


@dataclass
class FileValidation:
    """Dataclass representing file validation information

            Variables:
                event (FileValidationTypes | None): Type of file validation event
                file_location (str | None): Location of the file to be validated
                result (ValidationResult | None): Result of the file validation (PASS/FAIL)
                timestamp (str | None): Timestamp when the file validation occurred
        """

    event: Optional[FileValidationTypes]
    file_location: Optional[str]
    result: Optional[ValidationResultTypes]
    timestamp: Optional[str]


@dataclass
class SchemaValidation:
    """Dataclass representing schema validation information

            Variables:
                 event (FileValidationTypes | None): Type of schema validation event
                 value (any): Value
                 result (ValidationResult | None): Result of the file validation (PASS/FAIL)
        """
    event: Optional[SchemaValidationTypes]
    value: Optional[any]
    result: Optional[ValidationResultTypes]


@dataclass
class ColumnValidation:
    """Dataclass representing column validation information

           Variables:
                event (ColumnValidationTypes | None): Type of column validation event
                columns (List[str] | None): List of columns to be validated
                result (ValidationResult | None): Result of the column validation (PASS/FAIL)
                timestamp (str | None): Timestamp when the column validation occurred
        """
    event: Optional[ColumnValidationTypes]
    columns: Optional[List[str]]
    result: Optional[ValidationResultTypes]
    timestamp: Optional[str]


@dataclass
class DataValidation:
    """Dataclass representing data validation information

           Variables:
                event (DataValidationTypes | None): Type of data validation event
                column (str | None): Specific column for data validation
                rows (List[str] | None): List of rows to be validated
                result (ValidationResult | None): Result of the data validation (PASS/FAIL)
                timestamp (str | None): Timestamp when the data validation occurred
        """
    event: Optional[DataValidationTypes]
    column: Optional[str]
    rows: Optional[List[str]]
    result: Optional[ValidationResultTypes]
    timestamp: Optional[str]
