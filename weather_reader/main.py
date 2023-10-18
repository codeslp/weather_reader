from download_seq import download_many as download_readings_seq
from download_concur import download_many as download_readings_concur
from validate_reading import validate_reading
from download_common import (
    save_to_pq,
    initial_report,
    final_report,
    SERVERS,
    city_lat_lon,
)
import time

def main(city_lat_lon, concur_type=None, max_concur_req=None):
    initial_report((concur_type, max_concur_req), city_lat_lon)
    t0 = time.perf_counter()

    if concur_type == "thread":
        result = download_readings_concur(
            SERVERS["WEATHER"], city_lat_lon, "thread", max_concur_req
        )
        df = result[0]
        counter = result[1]
    elif concur_type == "process":
        if __name__ == '__main__':
            result = download_readings_concur(
                SERVERS["WEATHER"], city_lat_lon, "process", max_concur_req
            )
        df = result[0]
        counter = result[1]
    elif concur_type == "coroutine":
        if __name__ == '__main__':
            result = download_readings_async(
                SERVERS["WEATHER"], city_lat_lon, max_concur_req
            )
        df = result[0]
        counter = result[1]
    else:
        if __name__ == '__main__':
            result = download_readings_seq(SERVERS["WEATHER"], city_lat_lon)
        df = result[0]
        counter = result[1]

    valid_readings_batch = validate_reading(df)
    save_to_pq(valid_readings_batch)
    final_report(counter, t0)

    return df

if __name__ == '__main__':

    print(main(city_lat_lon, "thread"))
