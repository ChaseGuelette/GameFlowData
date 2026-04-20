import json
from typing import Any

import requests


def fetch_data(url: str) -> dict[str, Any]:
    """
    Makes a GET request to the specified URL and returns JSON data.

    Args:
        url: The URL to make the GET request to

    Returns:
        Dictionary containing the JSON response data

    Raises:
        requests.RequestException: If the request fails
        json.JSONDecodeError: If the response cannot be parsed as JSON
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.RequestException as e:
        raise requests.RequestException(f"Failed to fetch data from {url}: {str(e)}")
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Failed to parse JSON response: {str(e)}", e.doc, e.pos)

def process_data(data: dict[str, Any]) -> list[Any]:
    """
    Extracts all values from the dictionary where the key starts with "score_".

    Args:
        data: Dictionary containing the data to process

    Returns:
        List of values where keys start with "score_"
    """
    score_values = []
    for key, value in data.items():
        if key.startswith("score_"):
            score_values.append(value)
    return score_values
