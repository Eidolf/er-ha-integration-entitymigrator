import logging
import urllib.parse
from datetime import datetime
import requests

_LOGGER = logging.getLogger(__name__)

class InfluxV1Migrator:
    def __init__(self, host, port, database, username=None, password=None, ssl=False):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.ssl = ssl
        self.base_url = f"{'https' if ssl else 'http'}://{host}:{port}"
        
        # Prepare auth
        self.session = requests.Session()
        if username and password:
            self.session.auth = (username, password)

    def query(self, q):
        """Execute an InfluxQL query."""
        url = f"{self.base_url}/query"
        params = {"db": self.database, "q": q}
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            _LOGGER.error("InfluxDB query failed: %s (Query: %s)", e, q)
            raise e

    def write_lines(self, lines):
        """Write Line Protocol points to InfluxDB."""
        url = f"{self.base_url}/write"
        params = {"db": self.database, "precision": "ns"}
        data = "\n".join(lines) + "\n"
        try:
            resp = self.session.post(url, params=params, data=data, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            _LOGGER.error("InfluxDB write failed: %s", e)
            raise e

    def discover_series_and_counts(self, old_entity):
        """Find measurements containing the entity and count total data points."""
        series_info = []
        total_points = 0
        
        # Show all series matching the entity ID
        q = f"SHOW SERIES WHERE \"entity_id\" = '{old_entity}'"
        result = self.query(q)
        
        measurements = set()
        results = result.get("results", [])
        if results and "series" in results[0]:
            for series in results[0]["series"]:
                for val in series.get("values", []):
                    # Series value format is usually "measurement,tag1=val1,tag2=val2"
                    series_str = val[0]
                    measurement = series_str.split(",")[0]
                    measurements.add(measurement)

        for measurement in sorted(list(measurements)):
            # Get count of points for this measurement
            count_q = f"SELECT COUNT(*) FROM \"{measurement}\" WHERE \"entity_id\" = '{old_entity}'"
            count_res = self.query(count_q)
            count = 0
            res_results = count_res.get("results", [])
            if res_results and "series" in res_results[0]:
                series_data = res_results[0]["series"][0]
                # Find the value column index
                val_idx = -1
                for idx, col in enumerate(series_data.get("columns", [])):
                    if col != "time":
                        val_idx = idx
                        break
                if val_idx != -1 and series_data.get("values"):
                    count = int(series_data["values"][0][val_idx])

            if count > 0:
                series_info.append({"measurement": measurement, "count": count})
                total_points += count

        return series_info, total_points

    def migrate_entity_data(self, old_entity, new_entity, delete_old=False, progress_callback=None):
        """Read points of old_entity, update tag to new_entity, write back, and optionally delete old."""
        series_info, total_points = self.discover_series_and_counts(old_entity)
        if total_points == 0:
            return {"status": "Success", "copied": 0, "deleted": 0}

        copied_count = 0
        deleted_count = 0

        for s in series_info:
            measurement = s["measurement"]
            
            # 1. Discover Tag Keys and Field Keys to correctly parse them later
            tag_keys = set()
            tag_res = self.query(f"SHOW TAG KEYS FROM \"{measurement}\"")
            results = tag_res.get("results", [])
            if results and "series" in results[0]:
                for val in results[0]["series"][0].get("values", []):
                    tag_keys.add(val[0])
            
            field_keys = set()
            field_res = self.query(f"SHOW FIELD KEYS FROM \"{measurement}\"")
            results = field_res.get("results", [])
            if results and "series" in results[0]:
                for val in results[0]["series"][0].get("values", []):
                    field_keys.add(val[0])

            # Always treat entity_id as a tag
            tag_keys.add("entity_id")

            # 2. Fetch and migrate points in chunks of 5000
            chunk_size = 5000
            offset = 0
            while True:
                q = f"SELECT * FROM \"{measurement}\" WHERE \"entity_id\" = '{old_entity}' LIMIT {chunk_size} OFFSET {offset}"
                data_res = self.query(q)
                
                results = data_res.get("results", [])
                if not results or "series" not in results[0]:
                    break
                
                series_data = results[0]["series"][0]
                columns = series_data["columns"]
                values = series_data["values"]
                
                if not values:
                    break

                lines_to_write = []
                for val_row in values:
                    row_dict = dict(zip(columns, val_row))
                    
                    # Convert time to nanosecond epoch
                    # InfluxQL returns ISO time strings e.g. "2023-07-12T15:23:27Z"
                    time_str = row_dict["time"]
                    # Strip Z and parse
                    if time_str.endswith("Z"):
                        time_str = time_str[:-1]
                    
                    # Handle varying subsecond lengths
                    if "." in time_str:
                        base, sub = time_str.split(".")
                        sub = sub[:6]  # limit to microseconds
                        time_str = f"{base}.{sub}"
                        dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%f")
                    else:
                        dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
                    
                    ns_timestamp = int(dt.timestamp() * 1e9)

                    # Build tags and fields
                    tags = {}
                    fields = {}
                    
                    for col_name, col_val in row_dict.items():
                        if col_name == "time" or col_val is None:
                            continue
                        
                        if col_name in tag_keys:
                            # Map old entity ID to new entity ID
                            val_str = str(col_val)
                            if col_name == "entity_id" and val_str == old_entity:
                                val_str = new_entity
                            
                            # Escape tag key and value
                            tag_k = col_name.replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")
                            tag_v = val_str.replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")
                            tags[tag_k] = tag_v
                        elif col_name in field_keys:
                            # Format field values
                            if isinstance(col_val, bool):
                                fields[col_name] = "t" if col_val else "f"
                            elif isinstance(col_val, (int, float)):
                                fields[col_name] = str(col_val)
                            else:
                                # String field: escape quotes and surround with quotes
                                escaped_val = str(col_val).replace('"', '\\"')
                                fields[col_name] = f'"{escaped_val}"'

                    if fields:
                        # Format tags: comma separated k=v
                        tag_str = ""
                        if tags:
                            tag_str = "," + ",".join([f"{k}={v}" for k, v in sorted(tags.items())])
                        
                        # Format fields: comma separated k=v
                        field_str = ",".join([f"{k}={v}" for k, v in fields.items()])
                        
                        # Escaped measurement name
                        meas_name = measurement.replace(" ", "\\ ").replace(",", "\\,")
                        
                        line = f"{meas_name}{tag_str} {field_str} {ns_timestamp}"
                        lines_to_write.append(line)

                if lines_to_write:
                    self.write_lines(lines_to_write)
                    copied_count += len(lines_to_write)
                    if progress_callback:
                        progress_callback(copied_count, total_points)

                if len(values) < chunk_size:
                    break
                offset += chunk_size

            # 3. Optionally delete the old entity data from this measurement
            if delete_old and copied_count > 0:
                del_q = f"DELETE FROM \"{measurement}\" WHERE \"entity_id\" = '{old_entity}'"
                self.query(del_q)
                deleted_count += s["count"]

        return {
            "status": "Success",
            "copied": copied_count,
            "deleted": deleted_count
        }
