"""Module for reading Excel files into pandas DataFrames."""
import logging
import time
from io import BytesIO, StringIO
from typing import Protocol

import pandas as pd
from fastapi import UploadFile
from xlsx2csv import Xlsx2csv

# Configure logging
logger = logging.getLogger(__name__)


class ExcelConverter(Protocol):
    """Protocol for Excel conversion strategies."""
    
    async def convert(self, file_contents: bytes) -> pd.DataFrame:
        """Convert Excel file contents to DataFrame.
        
        Args:
            file_contents: Raw bytes of the Excel file
            
        Returns:
            Pandas DataFrame containing the Excel data
        """
        ...


class Xlsx2csvConverter:
    """Excel converter implementation using xlsx2csv library."""
    
    async def convert(self, file_contents: bytes) -> pd.DataFrame:
        """Convert Excel file to DataFrame using xlsx2csv.
        
        Args:
            file_contents: Raw bytes of the Excel file
            
        Returns:
            Pandas DataFrame containing the Excel data
        """
        output = StringIO()
        Xlsx2csv(BytesIO(file_contents), outputencoding="utf-8").convert(output)
        
        output.seek(0)
        return pd.read_csv(output)


class PandasConverter:
    """Excel converter implementation using pandas directly."""
    
    async def convert(self, file_contents: bytes) -> pd.DataFrame:
        """Convert Excel file to DataFrame using pandas.
        
        Args:
            file_contents: Raw bytes of the Excel file
            
        Returns:
            Pandas DataFrame containing the Excel data
        """
        return pd.read_excel(BytesIO(file_contents))


class FileReader:
    """Class for reading Excel files and converting them to DataFrames."""
    
    def __init__(self, converter: ExcelConverter = None):
        """Initialize with converter strategy.
        
        Args:
            converter: Strategy for converting Excel files to DataFrames
        """
        self.converter = converter or Xlsx2csvConverter()
    
    async def read_excel(self, file: UploadFile) -> pd.DataFrame:
        """Read Excel file and return as DataFrame.
        
        Uses the configured converter strategy to read the file efficiently.
        
        Args:
            file: Uploaded Excel file
            
        Returns:
            Pandas DataFrame containing the Excel data
        """
        start_time = time.time()
        
        contents = await file.read()
        df = await self.converter.convert(contents)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Excel file read in {elapsed_time:.3f} seconds using {self.converter.__class__.__name__}")
        
        return df