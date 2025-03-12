"""Module for data validation against predefined rules."""
import logging
import time
from collections import defaultdict
from typing import Dict, Any, Optional, Type, Union, List

import pandas as pd
from fastapi import HTTPException
from tqdm import tqdm

from schemas.validation_response import ValidationResponse

# Configure logging
logger = logging.getLogger(__name__)


class DataTypeValidator:
    """Base class for data type validation strategies."""
    
    @classmethod
    def validate(cls, series: pd.Series) -> List[int]:
        """Validate a pandas Series against a specific data type.
        
        Args:
            series: The pandas Series to validate
            
        Returns:
            List of invalid row indices
        """
        raise NotImplementedError("Subclasses must implement validate method")


class NumericValidator(DataTypeValidator):
    """Validator for numeric types (int, float)."""
    
    @classmethod
    def validate(cls, series: pd.Series) -> List[int]:
        numeric_series = pd.to_numeric(series, errors="coerce")
        return series.index[numeric_series.isna()].tolist()
    
    @classmethod
    def convert(cls, series: pd.Series) -> pd.Series:
        """Convert series to numeric type."""
        return pd.to_numeric(series, errors="coerce")


class DateTimeValidator(DataTypeValidator):
    """Validator for datetime types."""
    
    @classmethod
    def validate(cls, series: pd.Series) -> List[int]:
        datetime_series = pd.to_datetime(series, errors="coerce")
        return series.index[datetime_series.isna()].tolist()
    
    @classmethod
    def convert(cls, series: pd.Series) -> pd.Series:
        """Convert series to datetime type."""
        return pd.to_datetime(series, infer_datetime_format=True, errors="coerce")


class StringValidator(DataTypeValidator):
    """Validator for string types."""
    
    @classmethod
    def validate(cls, series: pd.Series) -> List[int]:
        return series.index[~series.apply(lambda x: isinstance(x, str))].tolist()


class GenericValidator(DataTypeValidator):
    """Validator for other Python types."""
    
    def __init__(self, expected_type: Type):
        self.expected_type = expected_type
    
    def validate(self, series: pd.Series) -> List[int]:
        return series.index[~series.apply(lambda x: isinstance(x, self.expected_type))].tolist()


class ValidatorFactory:
    """Factory for creating appropriate validators based on expected data type."""
    
    @staticmethod
    def get_validator(expected_type: Union[Type, str]) -> DataTypeValidator:
        """Return appropriate validator based on expected type.
        
        Args:
            expected_type: The expected data type
            
        Returns:
            An appropriate validator instance
        """
        if expected_type in {int, float}:
            return NumericValidator()
        elif expected_type in {pd.Timestamp, "datetime"}:
            return DateTimeValidator()
        elif expected_type == str:
            return StringValidator()
        else:
            return GenericValidator(expected_type)


class Validator:
    """Data validator that checks DataFrame against predefined rules."""
    
    def __init__(self, validation_rules: Dict[str, Dict[str, Any]]) -> None:
        """Initialize with validation rules.
        
        Args:
            validation_rules: Dictionary mapping rule IDs to validation rules
        """
        self.validation_rules = validation_rules
        self.validator_factory = ValidatorFactory()
    
    def validate(self, df: pd.DataFrame, rule_id: str) -> ValidationResponse:
        """Validate DataFrame against rules for the specified ID.
        
        Args:
            df: DataFrame to validate
            rule_id: ID of validation rules to apply
            
        Returns:
            ValidationResponse with errors and validated data
            
        Raises:
            HTTPException: If rule_id is invalid or required columns are missing
        """
        start_time = time.time()
        
        # Check if rule_id exists
        if rule_id not in self.validation_rules:
            raise HTTPException(status_code=400, detail=f"Invalid rule ID: {rule_id}")
        
        # Get rules for this ID
        rules = self.validation_rules[rule_id]
        column_types = rules.get("dtype", {})
        required_columns = list(column_types.keys())
        
        # Check for missing columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {missing_columns}"
            )
        
        # Validate each column
        df_copy = df.copy()
        errors = self._validate_columns(df_copy, column_types)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Validation completed in {elapsed_time:.3f} seconds for rule ID {rule_id}")
        
        # Return validation response
        if not errors:
            return ValidationResponse(errors={}, data=df_copy.to_dict(orient="records"))
        return ValidationResponse(errors=errors, data=[])
    
    def _validate_columns(
        self, df: pd.DataFrame, column_types: Dict[str, Any]
    ) -> Dict[int, List[str]]:
        """Validate columns against their expected types.
        
        Args:
            df: DataFrame to validate
            column_types: Dictionary mapping column names to expected types
            
        Returns:
            Dictionary mapping row indices to lists of invalid columns
        """
        errors = defaultdict(list)
        
        for col, expected_type in column_types.items():
            if col not in df.columns:
                continue
            
            validator = self.validator_factory.get_validator(expected_type)
            invalid_rows = validator.validate(df[col])
            
            # Apply type conversion where applicable
            if hasattr(validator, 'convert'):
                df[col] = validator.convert(df[col])
            
            # Record errors
            if invalid_rows:
                for row_idx in tqdm(invalid_rows, desc=f"Processing errors in {col}"):
                    errors[row_idx].append(col)
        
        return dict(errors)