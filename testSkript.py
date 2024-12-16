from datetime import datetime
import here_location_services.config.matrix_routing_config
import time


def is_iso_format(date_str):
    try:
        # Versuche, den String mit fromisoformat zu parsen
        datetime.fromisoformat(date_str)
        return True
    except ValueError:
        return False


start = time.time()
print(f"Duration of generating input: {time.time() -start} seconds")
