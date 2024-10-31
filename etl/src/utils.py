import logging

import requests


def setup_logger(name, level=logging.DEBUG):
    """
    Sets up a logger with the specified name and logging level.

    Args:
        name (str): The name of the logger.
        level (int): The logging level (default: logging.DEBUG).

    Returns:
        logging.Logger: Configured logger object.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    console_handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(console_handler)

    return logger


def fetch_shifts():
    """
    Fetches all shifts from the API in a paginated manner.

    This function repeatedly sends GET requests to the specified API endpoint
    until all available shifts are retrieved.

    Returns:
        list: A list of shift records fetched from the API.
    """
    logger = setup_logger("fetch_shifts")
    url = "http://localhost:8000/api/shifts"
    all_shifts = []

    try:
        while True:
            response = requests.get(url)
            response_json = response.json()

            if response.status_code == 200:
                all_shifts.extend(response_json["results"])
            else:
                logger.error(
                    f"Error. Status code: {response.status_code}. Response: {response.text}"
                )
                raise Exception("Error fetching shifts.")

            if not response_json.get("links", {}).get("next"):
                break

            url = f"{response_json["links"]["base"]}{response_json["links"]["next"]}"
    except Exception as e:
        logger.error("Error fetching shifts.")
        raise Exception(repr(e))

    logger.info("Successfully fetched shifts")

    return all_shifts
