# import time
# import logging
# import pandas as pd
# import io
# from fastapi import UploadFile


# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)


# class FileReader:
#     """Lớp FileReader để đọc file Excel và chuyển thành DataFrame."""
    
#     async def read_excel(self, file: UploadFile) -> pd.DataFrame:
#         """Đọc file Excel và trả về DataFrame."""
#         start_time = time.time()
#         contents = await file.read()
#         df = pd.read_excel(io.BytesIO(contents))
#         elapsed_time = time.time() - start_time
#         logger.info(f"Thời gian đọc file Excel: {elapsed_time:.3f} giây")
#         return df

import time
import logging
import pandas as pd
import io
from fastapi import UploadFile
from xlsx2csv import Xlsx2csv
from io import StringIO

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class FileReader:
    """Lớp FileReader để đọc file Excel và chuyển thành DataFrame nhanh hơn bằng xlsx2csv."""
    
    async def read_excel(self, file: UploadFile) -> pd.DataFrame:
        """Đọc file Excel nhanh bằng xlsx2csv và trả về DataFrame."""
        start_time = time.time()
        contents = await file.read()
        
        # Chuyển đổi file Excel thành CSV trong bộ nhớ
        output = StringIO()
        Xlsx2csv(io.BytesIO(contents), outputencoding="utf-8").convert(output)
        
        # Đọc CSV vào DataFrame
        output.seek(0)
        df = pd.read_csv(output)

        print(df['test1'])
        elapsed_time = time.time() - start_time
        logger.info(f"Thời gian đọc file Excel (dùng xlsx2csv): {elapsed_time:.3f} giây")
        
        return df
