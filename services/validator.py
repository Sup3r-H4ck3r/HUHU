import pandas as pd
import time
import logging
from tqdm import tqdm
from collections import defaultdict
from typing import Dict, Any, List, Optional
from fastapi import HTTPException
from collections import defaultdict
from schemas.validation_response import ValidationResponse


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Validator:
    def __init__(self, validation_rules: Dict[str, Dict[str, Any]]) -> None:
        self.validation_rules = validation_rules

    def validate(self,
                 df: pd.DataFrame,
                 id: str) -> Optional[ValidationResponse]:
        """Xác thực dữ liệu DataFrame theo quy tắc của ID."""
        start_time = time.time()

        if id not in self.validation_rules:
            raise HTTPException(status_code=400, detail="Invalid ID")
        
        rules = self.validation_rules[id]
        required_columns = list(rules.get("dtype", {}).keys())
        column_types = rules.get("dtype", {})
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(status_code=400,
                                detail=f"Missing required columns: {missing_columns}"
                                )

        errors = defaultdict(list)
        for col, expected_type in column_types.items():
            if col not in df.columns:
                continue

            invalid_rows = []
            if expected_type in {int, float}:
                numeric_series = pd.to_numeric(df[col], errors="coerce")
                invalid_rows = df.index[numeric_series.isna()].tolist()
                df[col] = numeric_series
            elif expected_type in {pd.Timestamp, "datetime"}:
                datetime_series = pd.to_datetime(df[col], errors="coerce")
                invalid_rows = df.index[datetime_series.isna()].tolist()
                df[col] = datetime_series
            elif expected_type == str:
                invalid_rows = df.index[~df[col].apply(lambda x: isinstance(x, str))].tolist()
            else:
                invalid_rows = df.index[~df[col].apply(lambda x: isinstance(x, expected_type))].tolist()
            
            if invalid_rows:
                for irow in tqdm(invalid_rows):
                    errors[irow].append(col)

        elapsed_time = time.time() - start_time
        logger.info(f"Validation completed in {elapsed_time:.3f} seconds for id {id}")
        if errors == {}:
            return ValidationResponse(errors=errors, data=df.to_dict(orient="records"))
        return ValidationResponse(errors=errors, data=[])