from plink_integration import plink_roh, plink_parentage
from zip_file_handler import unzip_file
import polars as pl
from fastapi import FastAPI, UploadFile, HTTPException
from pathlib import Path
import shutil
import uvicorn

def test(tped: pl.DataFrame):
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

    plink_roh(input_file=str(file_path))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)