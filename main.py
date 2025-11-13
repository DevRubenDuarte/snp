from plink_integration import plink_roh, plink_parentage
from zip_file_handler import unzip_file
import polars as pl
from fastapi import FastAPI, UploadFile, HTTPException
from pathlib import Path
import os
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
    results_folder = Path(f"roh/{dog_id}")

    try:
        # Create directories if they don't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)
        results_folder.mkdir(parents=True, exist_ok=True)
        results_file_path = results_folder / f"{dog_id}_roh"

        # Save file and unzip if necessary
        extracted_path, _ = _save_file_and_unzip(file, file_path)

        # Find .tped file
        matches = []
        for f in os.listdir(extracted_path):
            if f.endswith(".tped"):
                matches.append(os.path.join(extracted_path, f))
                break

        if not matches:
            raise HTTPException(status_code=400, detail="No .tped file found in the uploaded content")
        if len(matches) > 1:
            raise HTTPException(status_code=400, detail="Multiple .tped files found; please upload only one")

        # completes path and removes extension
        tped_file = Path(os.path.splitext(matches[0])[0])

        # Call plink_roh function
        plink_roh(tped_file, results_file_path)

        # Return success response
        return {
            "status": "success",
            "message": "ROH analysis completed successfully",
            "dog_id": dog_id,
            "output_folder": str(results_folder)
        }

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
    output_genome_file = Path(f"ibd/{dog_id}")

    try:
        # Create parent directory if it doesn't exist
        offspring_file_path.parent.mkdir(parents=True, exist_ok=True)
        output_genome_file.mkdir(parents=True, exist_ok=True)

        # Save files and unzip if necessary
        if not offspring_file.filename or not parent1_file.filename or not parent2_file.filename:
            raise HTTPException(status_code=400, detail="One or more uploaded files are missing filenames")

        e_path_offspring, offspring_contents = _save_file_and_unzip(offspring_file, offspring_file_path)
        e_path_parent1, parent1_contents = _save_file_and_unzip(parent1_file, parent1_file_path)
        e_path_parent2, parent2_contents = _save_file_and_unzip(parent2_file, parent2_file_path)
        extracted_path_offspring = e_path_offspring
        extracted_path_parent1 = e_path_parent1
        extracted_path_parent2 = e_path_parent2

        matches = []
        for f in os.listdir(extracted_path_offspring):
            if f.endswith(".tped"):
                matches.append(os.path.join(extracted_path_offspring, f))
                break

        # Expects 3 files: offspring, parent1, parent2
        if not len(matches) == 3:
            raise HTTPException(status_code=400, detail=f"Expected 3 .tped files, found {len(matches)} files")

        # Call plink_parentage function
        plink_parentage(extracted_path_offspring, extracted_path_parent1, extracted_path_parent2, output_genome_file)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _save_file_and_unzip(file: UploadFile, file_path: Path) -> tuple[Path, dict[str, bytes]]:
    """
    Save a file and unzip

    Returns the path to the extracted folder
    """

    with Path(file_path).open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    if file.filename:
        if file.filename.endswith(".zip"):
            extracted_folder, contents = unzip_file(file_path, file_path.parent)
            return extracted_folder, contents
        else:
            raise HTTPException(status_code=400, detail="Uploaded file is not a zip file")
    else:
        raise HTTPException(status_code=400, detail="Uploaded file is missing filename")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

