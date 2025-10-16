from db_connection import add_to_tbl_loci, add_to_tbl_alleles

import pandas as pd
from fastapi import FastAPI, UploadFile, HTTPException
from pathlib import Path
import shutil
import uvicorn

# def main():
    # # Load the TPED file
    # tped_file = pd.read_csv("database/barbel_ex/test.tped", sep=" ", header=None)
    # # Create tables
    # add_to_tbl_loci(tped_file)
    # add_to_tbl_alleles(tped_file, dog=int(1), source=1)

def test(tped: pd.DataFrame):
    print (tped.head())

app = FastAPI(title="File Upload API")

# Create uploads directory
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

@app.post("/upload")
async def upload_file(file: UploadFile, dog_id: int) -> None:
    """
    Upload a file with dog ID
    """

    file_path = UPLOAD_DIR / f"dog_{dog_id}_{file.filename}"

    try:
        # Save file
        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    add_to_tbl_loci(pd.read_csv(file_path, sep=" ", header=None))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)