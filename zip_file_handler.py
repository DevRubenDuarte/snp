import zipfile
import os
from pathlib import Path

def unzip_file(file_path: Path, output_folder: Path = Path("uploads")) -> tuple[Path, dict[str, bytes]]:
    """Unzips a zip file to the specified output folder.

    Args:
        file_path (str): The path to the zip file.
        output_folder (str): The folder where the contents will be extracted.

    Returns:
        tuple[str, dict[str, bytes]]: A tuple containing the path to the extracted folder and a dictionary
            of file names and their contents.
    """

    zip_name = os.path.basename(file_path)
    os.makedirs(output_folder, exist_ok=True)  # Ensure the subfolder exists
    contents = {}

    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            with zip_ref.open(file_name) as file:
                file_content = file.read()
                contents[file_name] = file_content

                # Save the file to the target folder
                with open(os.path.join(output_folder, file_name), 'wb') as output_file:
                    output_file.write(file_content)

    print("Unzipping completed. Contents:", contents.keys())
    return output_folder, contents