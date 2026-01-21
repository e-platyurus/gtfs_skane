import sqlite3

from typing import TypedDict, List

class Route(TypedDict):
    id: str
    short_name: str
    long_name: str
    description: str
    type: int
    direction: int
    agency_id: str
    agency_name: str
    agency_url: str
    headsign: str

class Stop(TypedDict):
    id: str
    name: str
    lat: str
    lon: str
    location_type: str
    parent_station: str
    timezone: str
    wheelchair_boarding: str
    platform_code: str
    routes: List[Route]

class GTFSBackend:
    def __init__(self, db_path: str, stop_id: str):
        self._db_path = db_path
        self._stop_id = stop_id
        self._stop: Stop | None = None
        self._routes: List[Route] = []
        self._db_state = "noDb"
        self._departures = []
        self._active_routes: List[str] = []

    def get_next_departure(self) -> dict:
        """Get the next departures for the given schedule."""
        sql_query = f"""
            WITH baseData AS (
                SELECT 
                    CASE WHEN date(st.departure_time) = '1970-01-01' 
                        THEN date('now', '-1 day') || ' ' || time(st.departure_time)
                        ELSE date('now') || ' ' || time(st.departure_time)
                    END AS correctDepartureDateTime,
                    st.stop_headsign
                FROM trips t
                INNER JOIN stop_times st ON st.trip_id = t.trip_id
                INNER JOIN stops s ON st.stop_id = s.stop_id
                WHERE t.route_id = :route_id
                AND st.stop_id = :origin_station_id
                AND t.direction_id = 1
                AND t.service_id in (SELECT service_id from calendar_dates where date = date('now', '-1 day'))
                UNION
                SELECT 
                    CASE WHEN date(st.departure_time) = '1970-01-01' 
                        THEN date('now') || ' ' || time(st.departure_time)
                        ELSE date('now', '+1 day') || ' ' || time(st.departure_time)
                    END AS correctDepartureDateTime,
                    st.stop_headsign
                FROM trips t
                INNER JOIN stop_times st ON st.trip_id = t.trip_id
                INNER JOIN stops s ON st.stop_id = s.stop_id
                WHERE t.route_id = :route_id
                AND st.stop_id = :origin_station_id
                AND t.direction_id = 1
                AND t.service_id in (SELECT service_id from calendar_dates where date = date('now'))
                UNION
                SELECT 
                    CASE WHEN date(st.departure_time) = '1970-01-01' 
                        THEN date('now', '+1 day') || ' ' || time(st.departure_time)
                        ELSE date('now', '+2 day') || ' ' || time(st.departure_time)
                    END AS correctDepartureDateTime,
                    st.stop_headsign
                FROM trips t
                INNER JOIN stop_times st ON st.trip_id = t.trip_id
                INNER JOIN stops s ON st.stop_id = s.stop_id
                WHERE t.route_id = :route_id
                AND st.stop_id = :origin_station_id
                AND t.direction_id = 1
                AND t.service_id in (SELECT service_id from calendar_dates where date = date('now', '+1 day'))
                ORDER BY correctDepartureDateTime ASC
                )
                SELECT 
                    correctDepartureDateTime AS departureDateTime,
                    time(correctDepartureDateTime) AS departureTime,
                    stop_headsign
                FROM baseData 
                WHERE correctDepartureDateTime > datetime('now', 'localtime')
                LIMIT 3
            """

        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row  # allows dict-like access
        try:
            cursor = conn.cursor()
            cursor.execute(
                sql_query,
                {
                    "origin_station_id": self._stop_id,
                    "route_id": "9011012001600000",
                },
            )
            rows = cursor.fetchall()

            departures = []
            for row in rows:
                departures.append({
                    "dateTime": row["departureDateTime"],
                    "time": row["departureTime"],
                    "headsign": row["stop_headsign"],
                })

            if len(departures) < 3:
                return None

            return {
                "nextDeparture": departures[0],
                "secondDeparture": departures[1],
                "thirdDeparture": departures[2],
            }
        finally:
            conn.close()


    def set_stop_info(self):
        sql_query = f"""
            SELECT 
                s.stop_id,
                s.stop_name,
                s.stop_lat,
                s.stop_lon,
                s.location_type,
                s.parent_station,
                s.wheelchair_boarding,
                s.platform_code,
                r.route_id,
                r.agency_id,
                a.agency_name,
                a.agency_url,
                a.agency_timezone,
                r.route_short_name,
                r.route_long_name,
                r.route_desc,
                r.route_type,
                t.direction_id,
                st.stop_headsign
            FROM stops s
            INNER JOIN stop_times st ON st.stop_id = s.stop_id
            INNER JOIN trips t ON st.trip_id = t.trip_id
            INNER JOIN routes r ON t.route_id = r.route_id
            INNER JOIN agency a ON r.agency_id = a.agency_id
            WHERE s.stop_id = :stop_id
            GROUP BY 
                s.stop_id,
                s.stop_name,
                s.stop_lat,
                s.stop_lon,
                s.location_type,
                s.parent_station,
                s.wheelchair_boarding,
                s.platform_code,
                r.route_id,
                r.agency_id,
                a.agency_name,
                a.agency_url,
                a.agency_timezone,
                r.route_short_name,
                r.route_long_name,
                r.route_desc,
                r.route_type,
                t.direction_id,
                st.stop_headsign
            """

        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row  # allows dict-like access
        try:
            cursor = conn.cursor()
            cursor.execute(
                sql_query,
                {
                    "stop_id": self._stop_id,
                },
            )
            rows = cursor.fetchall()
            if len(rows) == 0:
                self._routes = []
                self._stop = None
                return None


            for row in rows:
                self._active_routes.append(row["route_id"])
                self._routes.append(Route(
                    id = row["route_id"],
                    short_name = row["route_short_name"],
                    long_name = row["route_long_name"],
                    description = row["route_desc"],
                    type = row["route_type"],
                    direction = row["direction_id"],
                    agency_id = row["agency_id"],
                    agency_name = row["agency_name"],
                    agency_url = row["agency_url"],
                    headsign = row["stop_headsign"]
                ))
            general_data = rows[0]
            self._stop = Stop(
                id=general_data["stop_id"],
                name=general_data["stop_name"],
                lat=general_data["stop_lat"],
                lon=general_data["stop_lon"],
                location_type=general_data["location_type"],
                parent_station=general_data["parent_station"],
                timezone=general_data["agency_timezone"],
                wheelchair_boarding=general_data["wheelchair_boarding"],
                platform_code=general_data["platform_code"],
                routes=self._routes
            )

        finally:
            conn.close()

    def get_stop(self) -> Stop | None:
        return self._stop

    def get_routes(self) -> list[Route] | None:
        return self._routes
