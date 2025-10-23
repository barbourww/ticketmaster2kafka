import threading
import traceback
import json
import time
import os
import sys
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Tuple

import requests
# import gzip
# import shutil
# import rasterio
# from rasterio.transform import xy
# from pyproj import Transformer
# import matplotlib.pyplot as plt
from datetime import datetime
# from bs4 import BeautifulSoup
import numpy as np
# from pyproj import CRS
# from pyproj import Transformer

import kafka_confluent as kc
import psycopg as pg

import logging
logger = logging.getLogger(__name__)
# logging.getLogger('matplotlib.font_manager').disabled = True
from dotenv import load_dotenv
load_dotenv()

nashville_tz = ZoneInfo('US/Central')


def now_dtz():
    return dt.datetime.now(tz=nashville_tz)


# Helper function to wrap thread targets for fatal error handling
def thread_wrapper(target_func, args=(), name=""):
    def wrapped():
        try:
            target_func(*args)
        except Exception:
            logger.critical(f"Unhandled exception in thread '{name}', exiting entire process.", exc_info=True)
            traceback.print_exc(file=sys.stderr)
            sys.exit(1)
    return wrapped
    
from datetime import datetime, timedelta, timezone

def _iso_utc(dt: datetime) -> str:
    # Ticketmaster requires UTC with 'Z'
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# --- Database helpers (mirror weather DB env + retry/check) ---
def _connect_ticketmaster_database() -> pg.Connection:
    host = os.environ['SQL_HOSTNAME']
    port = os.environ['SQL_PORT']
    user = os.environ['SQL_USERNAME']
    password = os.environ['SQL_PASSWORD']
    database = 'NDOT'
    retry_counter = 5
    while retry_counter > 0:
        try:
            db_conn = pg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=database,
                autocommit=True
            )
            # basic check
            with db_conn.cursor() as cur:
                cur.execute("SELECT 1=1;")
                cur.fetchall()
            return db_conn
        except Exception as e:
            connection_error_context = e  # noqa: F841 (kept for parity with style)
            logger.warning("Could not connect database for Ticketmaster writing. Trying again....")
            retry_counter -= 1
            time.sleep(2)
    else:
        logger.error(f"Ticketmaster destination database parameters used were: "
                     f"host={host}, port={port}, dbname={database}, user={user}")
        raise pg.OperationalError("Could not connect database after all attempts.")

def _check_ticketmaster_database_connections(db_conn: pg.Connection) -> bool:
    try:
        with db_conn.cursor() as cur:
            cur.execute("SELECT 1=1;")
            cur.fetchall()
            return True
    except Exception:
        logger.warning("Ticketmaster database connection check failed. Attempting reconnect.")
        return False

# --- Core producer class (mirrors Weather*Producer pattern) ---
class TicketmasterEventsProducer:
    def __init__(self, base_url: str, poll_interval_minutes: int, kafka_config: Dict[str, Any],
                 query_params: Dict[str, Any] | None = None):
        self.base_url = base_url.rstrip('/')
        self.poll_interval_seconds = poll_interval_minutes * 60
        self.kc = kc.KafkaConfluentHelper(kafka_config)
        self.topic_name = os.environ.get('KAFKA_TOPIC_BASENAME', 'nashville-tm')
        self.partition_key = "0"
        self.api_key = os.environ['TICKETMASTER_API_KEY']
        # fetch params
        self.page_size = int(os.environ.get('TM_PAGE_SIZE', '200'))
        self.query_params: Dict[str, Any] = query_params or {}
        if 'countryCode' not in self.query_params:
            self.query_params['countryCode'] = os.environ.get('TM_COUNTRY_CODE', 'US')
        # optional time window
        if os.environ.get('TM_START_ISO'):
            self.query_params['startDateTime'] = os.environ['TM_START_ISO']
        if os.environ.get('TM_END_ISO'):
            self.query_params['endDateTime'] = os.environ['TM_END_ISO']
            
        # Optional absolute bounds still win
        if os.environ.get("TM_START_ISO"):
            self.query_params["startDateTime"] = os.environ["TM_START_ISO"]
        if os.environ.get("TM_END_ISO"):
            self.query_params["endDateTime"] = os.environ["TM_END_ISO"]

        # If neither absolute bound provided, derive from window
        if "startDateTime" not in self.query_params and "endDateTime" not in self.query_params:
            days_ahead = int(os.environ.get("TM_WINDOW_DAYS", "28"))
            now = datetime.now(timezone.utc)
            end = now + timedelta(days=days_ahead)
            # If you want to align to midnight UTC, uncomment next two lines:
            # now = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            # end = datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc)
            self.query_params["startDateTime"] = _iso_utc(now)
            self.query_params["endDateTime"]   = _iso_utc(end)
        
        # optional extra params: "classificationName=music,city=Nashville"
        extra = os.environ.get('TM_EXTRA_PARAMS')
        if extra:
            for kv in extra.split(','):
                if '=' in kv:
                    k,v = kv.split('=',1)
                    self.query_params[k.strip()] = v.strip()

        # Persistent DB connection for inserts
        self.db_conn = _connect_ticketmaster_database()

    # style parity: small wait helper
    def wait(self):
        time.sleep(self.poll_interval_seconds)

    # ---- API ----
    def _fetch_events(self) -> List[Dict[str, Any]]:
        """Page through events.json (guard deep paging)."""
        collected: List[Dict[str, Any]] = []
        size = max(1, min(200, self.page_size))
        page = 0
        while True:
            if size * page >= 1000:
                break
            params = dict(self.query_params, apikey=self.api_key, size=size, page=page)
            url = f"{self.base_url}/discovery/v2/events.json"
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 429:
                logger.warning("Ticketmaster rate limit hit (429). Backing off briefly.")
                time.sleep(2)
                continue
            r.raise_for_status()
            doc = r.json()
            events = (doc.get("_embedded") or {}).get("events", [])
            collected.extend(events)
            links = doc.get("_links", {})
            if "next" not in links:
                break
            page += 1
        return collected

    # ---- Mapping (flatten to tm_events row) ----
    @staticmethod
    def _pick_primary_image(images: List[Dict[str,Any]]) -> str | None:
        if not images: return None
        best = None; best_w = -1
        for im in images:
            w = im.get("width") or 0
            if im.get("ratio") == "16_9" and w > best_w:
                best = im; best_w = w
        if not best:
            for im in images:
                w = im.get("width") or 0
                if w > best_w:
                    best = im; best_w = w
        return best.get("url") if best else None

    @staticmethod
    def _pick_prices(e: dict) -> tuple[str | None, float | None, float | None]:
        pr = e.get("priceRanges") or []
        if not pr: return (None, None, None)
        std = next((x for x in pr if x.get("type") == "standard"), pr[0])
        return (std.get("currency"), std.get("min"), std.get("max"))

    @staticmethod
    def _flatten_event(e: dict) -> dict:
        dates = e.get("dates") or {}
        start  = (dates.get("start") or {})
        sales  = ((e.get("sales") or {}).get("public") or {})
        emb    = (e.get("_embedded") or {})
        venues = emb.get("venues") or []
        v0     = venues[0] if venues else {}
        city   = (v0.get("city") or {}).get("name")
        state  = (v0.get("state") or {})
        country= (v0.get("country") or {})
        atts   = emb.get("attractions") or []
        att_names = [a.get("name") for a in atts if a.get("name")]
        attraction_primary = att_names[0] if att_names else None
        attraction_names = "; ".join(att_names) if att_names else None
        cls    = e.get("classifications") or []
        c0     = next((c for c in cls if c.get("primary")), (cls[0] if cls else {}))
        img_url = TicketmasterEventsProducer._pick_primary_image(e.get("images") or [])
        currency, pmin, pmax = TicketmasterEventsProducer._pick_prices(e)

        return {
            "id": e["id"],
            "name": e.get("name"),
            "url": e.get("url"),
            "source": e.get("source"),
            "locale": e.get("locale"),
            "test": bool(e.get("test", False)),
            "status_code": (dates.get("status") or {}).get("code"),
            "timezone": dates.get("timezone"),
            "start_local_date": start.get("localDate"),
            "start_local_time": start.get("localTime"),
            "start_datetime_utc": start.get("dateTime"),
            "onsale_start_utc": sales.get("startDateTime"),
            "onsale_end_utc": sales.get("endDateTime"),
            "venue_id": v0.get("id"),
            "venue_name": v0.get("name"),
            "venue_address_line1": (v0.get("address") or {}).get("line1"),
            "city_name": city,
            "state_code": state.get("stateCode"),
            "country_code": country.get("countryCode"),
            "venue_postal_code": v0.get("postalCode"),
            "venue_timezone": v0.get("timezone"),
            "venue_lat": (v0.get("location") or {}).get("latitude"),
            "venue_lon": (v0.get("location") or {}).get("longitude"),
            "attraction_primary": attraction_primary,
            "attraction_names": attraction_names,
            "class_segment": (c0.get("segment") or {}).get("name"),
            "class_genre": (c0.get("genre") or {}).get("name"),
            "class_subgenre": (c0.get("subGenre") or {}).get("name"),
            "class_type": (c0.get("type") or {}).get("name"),
            "class_subtype": (c0.get("subType") or {}).get("name"),
            "image_url_primary": img_url,
            "price_currency": currency,
            "price_min": pmin,
            "price_max": pmax,
        }

    # ---- DB insert (UPSERT) ----
    def insert_events(self, events: List[dict]):
        if not _check_ticketmaster_database_connections(self.db_conn):
            self.db_conn = _connect_ticketmaster_database()

        insert_sql = """
            INSERT INTO laddms.tm_events (
              write_time, id, name, url, source, locale, test,
              status_code, timezone, start_local_date, start_local_time, start_datetime_utc,
              onsale_start_utc, onsale_end_utc,
              venue_id, venue_name, venue_address_line1, city_name, state_code, country_code,
              venue_postal_code, venue_timezone, venue_lat, venue_lon,
              attraction_primary, attraction_names,
              class_segment, class_genre, class_subgenre, class_type, class_subtype,
              image_url_primary, price_currency, price_min, price_max,
              first_seen_utc, last_seen_utc
            ) VALUES (
              %(write_time)s, %(id)s, %(name)s, %(url)s, %(source)s, %(locale)s, %(test)s,
              %(status_code)s, %(timezone)s, %(start_local_date)s, %(start_local_time)s, %(start_datetime_utc)s,
              %(onsale_start_utc)s, %(onsale_end_utc)s,
              %(venue_id)s, %(venue_name)s, %(venue_address_line1)s, %(city_name)s, %(state_code)s, %(country_code)s,
              %(venue_postal_code)s, %(venue_timezone)s, %(venue_lat)s, %(venue_lon)s,
              %(attraction_primary)s, %(attraction_names)s,
              %(class_segment)s, %(class_genre)s, %(class_subgenre)s, %(class_type)s, %(class_subtype)s,
              %(image_url_primary)s, %(price_currency)s, %(price_min)s, %(price_max)s,
              %(first_seen_utc)s, %(last_seen_utc)s
            );
        """
        
        now = now_dtz()  # your helper that returns tz-aware now

        def _f(v):
            try:
                return float(v) if v not in (None, "") else None
            except Exception:
                return None

        rows = []
        for ev in events:
            flat = self._flatten_event(ev)  # your existing mapper
            flat["venue_lat"] = _f(flat.get("venue_lat"))
            flat["venue_lon"] = _f(flat.get("venue_lon"))

            rows.append({
                "write_time": now,
                "first_seen_utc": now,
                "last_seen_utc": now,
                **flat
            })

        with self.db_conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
        
        # # batch executemany in the same style
#         write_time = now_dtz()
#         rows = []
#         for e in events:
#             flat = self._flatten_event(e)
#             rows.append({
#                 "write_time": write_time,
#                 "first_seen_utc": write_time,
#                 "last_seen_utc": write_time,
#                 **flat
#             })
#         if not rows:
#             logger.info("No Ticketmaster events to insert this cycle.")
#             return
#         with self.db_conn.cursor() as cur:
#             cur.executemany(insert_sql, rows)
        logger.info(f"Inserted/updated {len(rows)} rows into laddms.tm_events.")

    # ---- Kafka ----
    def produce_events_to_kafka(self, events):
        count = 0
        for e in events:
            payload = {"source":"ticketmaster","fetched_at": time.time(),"event": e}
            self.kc.send(topic=self.topic_name, key=self.partition_key,
                         json_data=json.dumps(payload),
                         headers=[('service', b'ticketmaster'), ('datatype', b'event')])
            count += 1
        logger.info(f"Produced {count} Ticketmaster events to Kafka.")


# --- Orchestration function (matches update_weather_* signature/flow) ---
def update_ticketmaster_events(base_url, poll_interval_minutes, receiver_kafka_config, query_params: dict | None = None):
    tm = TicketmasterEventsProducer(base_url, poll_interval_minutes, kafka_config=receiver_kafka_config,
                                    query_params=query_params)
    logger.info("Created new instance of Ticketmaster events receiver.")
    while True:
        # 1) pull
        try:
            events = tm._fetch_events()
            numEvents=len(events)
            print(f'Received {numEvents} events')
            print(json.dumps(events[0]))
            print('Trying to flatten an event...')
            print(tm._flatten_event(events[0]))
        except Exception as e:
            logger.error("Failed to pull Ticketmaster events.")
            logger.exception(e, exc_info=True)
            tm.wait()
            continue

        # 2) produce to Kafka
        try:
            tm.produce_events_to_kafka(events)
        except Exception as e:
            logger.error("Failed to send Ticketmaster events to Kafka.")
            logger.exception(e, exc_info=True)

        # 3) insert to DB
        try:
            tm.insert_events(events)
        except Exception as e:
            logger.error("Failed to insert Ticketmaster events.")
            logger.exception(e, exc_info=True)

        tm.wait()




if __name__ == "__main__":
    common_kafka_config = {
        'KAFKA_BOOTSTRAP': os.environ.get('KAFKA_BOOTSTRAP'),
        'KAFKA_USER':  os.environ.get('KAFKA_USER'),
        'KAFKA_PASSWORD': os.environ.get('KAFKA_PASSWORD'),
    }

    log_path = str(os.environ.get('LOG_PATH')) if os.environ.get('LOG_PATH') else "."
    loggerFile = log_path + '/ticketmaster2kafka.log'
    loggerFile = './ticketmaster2kafka.log'
    print('Saving logs to: ' + loggerFile)
    FORMAT = '%(asctime)s %(message)s'

    debug = True  # set to False to disable console logging

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.DEBUG if debug else logging.INFO)

    file_handler = logging.FileHandler(loggerFile)
    file_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    file_handler.setFormatter(logging.Formatter(FORMAT))
    root_logger.addHandler(file_handler)

    if debug:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter(FORMAT))
        root_logger.addHandler(console_handler)

    logger.info("Starting Ticketmaster to Kafka producer thread.")
    if True:
        # locations = [
        #     (float(os.environ.get('WEATHER_FORECAST_LAT')), float(os.environ.get('WEATHER_FORECAST_LON')))
        # ]
        # Match your weather thread creation style
        threading.Thread(target=thread_wrapper(
            update_ticketmaster_events,
            args=(
                "https://app.ticketmaster.com",      # base_url (kept as param to mirror weather’s style)
                int(os.environ.get('TM_POLL_MINS', '60')),   # poll interval (minutes)
                common_kafka_config,                 # Kafka helper config (same structure)
                # optional query params dict if you want to pass directly here:
                {'geoPoint': 'dn6m9qgn', 'radius': 1, 'units': 'km'}
                # {'countryCode': 'US', 'classificationName': 'music'}
            ),
            name="ticketmaster_events"
        )).start()

