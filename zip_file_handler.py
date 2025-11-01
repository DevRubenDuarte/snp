import zipfile
import os

def unzip_file(file_path: str, output_folder: str = "uploads") -> tuple[str, dict[str, bytes]]:
    """Unzips a zip file to the specified output folder.

    Args:
        file_path (str): The path to the zip file.
        output_folder (str): The folder where the contents will be extracted.

    Returns:
        str: The path to the folder containing the unzipped files.
    """

    zip_name = os.path.splitext(os.path.basename(file_path))[0]
    target_folder = os.path.join(output_folder, zip_name+"_unzipped")
    os.makedirs(target_folder, exist_ok=True)  # Ensure the subfolder exists
    contents = {}
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            file_path = os.path.join(target_folder, file_name)
            with zip_ref.open(file_name) as file:
                file_content = file.read()
                contents[file_name] = file_content
                # Save the file to the target folder
                with open(file_path, 'wb') as output_file:
                    output_file.write(file_content)
    print("Unzipping completed. Contents:", contents.keys())
    return target_folder, contents