import requests


def download_files(
    url, params: dict = {}, output_filename: str = "downloaded_file.json"
):
    response = requests.get(url, params=params)

    if response.status_code == 200:
        with open(output_filename, "wb") as file:
            file.write(response.content)
    else:
        raise ConnectionError(
            f"Failed to download file. Status code: {response.status_code}. "
            f"Message: {response.content}"
        )
