from plink_integration import plink_roh, plink_parentage
from zip_file_handler import unzip_file
from db_connection import process_zip, add_to_tbl_loci, add_to_tbl_alleles
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
async def upload_file(dog_id: int, file: UploadFile):
    """
    Upload a file with dog ID
    """
    file_path = UPLOAD_DIR / f"{dog_id}/{file.filename}"

    try:
        # Create parent directory if it doesn't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Save file
        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Return success response
        return {
            "status": "success",
            "message": "File uploaded successfully",
            "dog_id": dog_id,
            "filename": file.filename,
            "file_path": str(file_path)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/snp_roh")
async def process_roh(dog_id: int, file: UploadFile):
    """
    Upload and process a file to calculate ROH
    """
    file_path = UPLOAD_DIR / f"{dog_id}/{file.filename}"

    try:
        # Create parent directory if it doesn't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Save file and unzip if necessary
        extracted_path = _save_file_and_unzip(file, file_path)

        print(f"Extracted path: {extracted_path}")
        # # Call plink_roh function
        # plink_roh(str(extracted_path))

        # # Return success response
        # return {
        #     "status": "success",
        #     "message": "ROH analysis completed successfully",
        #     "dog_id": dog_id,
        #     "output_folder": str(UPLOAD_DIR / f"/roh/{dog_id}")
        # }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/snp_parentage")
async def process_parentage(dog_id: int, offspring_file: UploadFile, parent1_file: UploadFile,
                            parent2_file: UploadFile):
    """
    Upload and process one offspring and two parent files to calculate parentage
    """
    offspring_file_path = UPLOAD_DIR / f"{dog_id}/{offspring_file.filename}"
    parent1_file_path = UPLOAD_DIR / f"{dog_id}/{parent1_file.filename}"
    parent2_file_path = UPLOAD_DIR / f"{dog_id}/{parent2_file.filename}"
    output_genome_file = UPLOAD_DIR / f"ibd/{dog_id}"

    try:
        # Create parent directory if it doesn't exist
        offspring_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Save files and unzip if necessary
        extracted_path_offspring = _save_file_and_unzip(offspring_file, offspring_file_path)
        extracted_path_parent1 = _save_file_and_unzip(parent1_file, parent1_file_path)
        extracted_path_parent2 = _save_file_and_unzip(parent2_file, parent2_file_path)

        # Call plink_parentage function
        plink_parentage(extracted_path_offspring, extracted_path_parent1, extracted_path_parent2, output_genome_file)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _save_file_and_unzip(file: UploadFile, file_path: Path) -> Path:
    """
    Save a file and unzip if necessary

    Returns the path to the extracted folder
    """

    with Path(file_path).open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    if file.filename:
        if file.filename.endswith(".zip"):
            extracted_folder, _ = unzip_file(file_path, file_path.parent)
            return extracted_folder
        else:
            return file_path.parent
    else:
        return file_path.parent

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

