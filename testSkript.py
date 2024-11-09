from datetime import datetime
import here_location_services.config.matrix_routing_config


def is_iso_format(date_str):
    try:
        # Versuche, den String mit fromisoformat zu parsen
        datetime.fromisoformat(date_str)
        return True
    except ValueError:
        return False


a = "52.234, 28.923"
# Ursprüngliches Datum als String
b, c = float(a.split(",")[0]), float(a.split(",")[1])
print(b)
print(c)

from loguru import logger

page = 0
while (page + 1) * 100 < 550:
    if 550 <= 1:
        logger.warning("Dataframe-Shape warning David")
    print("MOIN")
    print((page) * 100, min((550 - page - 1 - page * 100, (page + 1) * 100)))
    # print(tmp_df)
    page += 1
