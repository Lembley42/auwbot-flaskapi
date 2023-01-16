from cryptography.fernet import Fernet
import json


def Decrypt_File(input_file:str, output_file:str, key:str) -> None:
    """Decrypts the contents of a file and writes the decrypted data to another file as a JSON string.

    Parameters:
    - input_file: str
        The path to the file containing the encrypted data. The file should be in binary mode ('rb').
    - output_file: str
        The path to the file where the decrypted data will be written. The file will be overwritten if it already exists.
    - key: str
        The key used to decrypt the data. This should be the same key that was used to encrypt the data.

    Returns:
        None"""

    # Read the encrypted data from the input file        
    with open(input_file, 'rb') as f:
        encrypted = f.read()
    
    # Decrypt the data
    fernet = Fernet(key.encode())
    decrypted_data = fernet.decrypt(encrypted)

    # Convert the decrypted data to a JSON string
    json_data = json.loads(decrypted_data)
    json_string = json.dumps(json_data, indent=4, ensure_ascii=False)

    # Write the JSON string to the output file
    with open(output_file, 'w') as f:
        f.write(json_string)

