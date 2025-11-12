from plink_integration import plink_roh, plink_parentage
from zip_file_handler import unzip_file
import polars as pl
from fastapi import FastAPI, UploadFile, HTTPException
from pathlib import Path
import shutil
import uvicorn

app = FastAPI(title="File Upload API")

# Create uploads directory
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

@app.post("/snp_upload")
async def upload_file(dog_id: int, file: UploadFile) -> None:
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

@app.post("/snp_roh")
async def process_roh(dog_id: int, file: UploadFile) -> None:
    """
    Upload and process a file to calculate ROH
    """

    file_path = UPLOAD_DIR / f"dog_{dog_id}_{file.filename}"

    try:
        extracted_path = _save_file_and_unzip(file, str(file_path))

        # Call plink_roh function
        plink_roh(str(extracted_path), output_folder=str(UPLOAD_DIR / f"dog_{dog_id}_roh"))

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/snp_parentage")
async def process_parentage(dog_id: int, offspring_file: UploadFile, parent1_file: UploadFile,
                            parent2_file: UploadFile) -> None:
    """
    Upload and process one offspring and two parent files to calculate parentage
    """

    offspring_file_path = UPLOAD_DIR / f"dog_{dog_id}_{offspring_file.filename}"
    parent1_file_path = UPLOAD_DIR / f"dog_{dog_id}_{parent1_file.filename}"
    parent2_file_path = UPLOAD_DIR / f"dog_{dog_id}_{parent2_file.filename}"
    output_genome_file=str(UPLOAD_DIR / f"dog_{dog_id}_parentage/genome_file")

    try:
        extracted_path_offspring = _save_file_and_unzip(offspring_file, str(offspring_file_path))
        extracted_path_parent1 = _save_file_and_unzip(parent1_file, str(parent1_file_path))
        extracted_path_parent2 = _save_file_and_unzip(parent2_file, str(parent2_file_path))

        # Call plink_parentage function
        plink_parentage(extracted_path_offspring, extracted_path_parent1, extracted_path_parent2, output_genome_file)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _save_file_and_unzip(file: UploadFile, file_path: str) -> str:
    """
    Save a file and unzip if necessary

    Returns the path to the extracted file or directory
    """
    extracted_path = ""
    with Path(file_path).open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    if file.filename:
        if file.filename.endswith(".zip"):
            extracted_path, _ = unzip_file(file.filename, str(UPLOAD_DIR))
    else:
        extracted_path = file_path

    return extracted_path

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

