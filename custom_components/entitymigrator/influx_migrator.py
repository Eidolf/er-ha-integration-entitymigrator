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

    def close(self):
        """Close the HTTP session to release resources and file descriptors."""
        try:
            self.session.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def ping(self):
        """Ping the InfluxDB server to test connection."""
        url = f"{self.base_url}/ping"
        try:
            resp = self.session.get(url, timeout=10)
            resp.raise_for_status()
            return True
        except Exception as e:
            _LOGGER.error("InfluxDB ping failed: %s", e)
            raise e

    def test_connection(self):
        """Test connection, auth, and database existence."""
        self.ping()
        self.query("SHOW RETENTION POLICIES", timeout=30)

    def query(self, q, timeout=30):
        """Execute an InfluxQL query."""
        import requests
        url = f"{self.base_url}/query"
        params = {"db": self.database, "q": q}
        resp = None
        try:
            resp = self.session.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout as e:
            _LOGGER.warning("InfluxDB query timed out (Query: %s). The operation will continue in the background on the InfluxDB server.", q)
            raise e
        except Exception as e:
            err_msg = str(e)
            if resp is not None and resp.text:
                err_msg = f"{e} - Details: {resp.text.strip()}"
            _LOGGER.error("InfluxDB query failed: %s (Query: %s)", err_msg, q)
            raise ValueError(err_msg) from e

    def write_lines(self, lines):
        """Write Line Protocol points to InfluxDB."""
        url = f"{self.base_url}/write"
        params = {"db": self.database, "precision": "ns"}
        data = "\n".join(lines) + "\n"
        resp = None
        try:
            resp = self.session.post(url, params=params, data=data, timeout=120)
            resp.raise_for_status()
        except Exception as e:
            err_msg = str(e)
            if resp is not None and resp.text:
                err_msg = f"{e} - Details: {resp.text.strip()}"
            _LOGGER.error("InfluxDB write failed: %s", err_msg)
            raise ValueError(err_msg) from e

    def get_all_entity_ids(self):
        """Get all unique entity_id tag values from InfluxDB."""
        try:
            res = self.query('SHOW TAG VALUES WITH KEY = "entity_id"')
            entity_ids = set()
            results = res.get("results", [])
            if results and "series" in results[0]:
                columns = results[0]["series"][0].get("columns", [])
                val_idx = columns.index("value") if "value" in columns else 1
                for val in results[0]["series"][0].get("values", []):
                    if len(val) > val_idx and val[val_idx]:
                        entity_ids.add(val[val_idx])
            return entity_ids
        except Exception as e:
            _LOGGER.error("Could not fetch entity_ids from InfluxDB: %s", e)
            return set()

    def delete_entity_series(self, entity_id):
        """Delete all series/measurements belonging to an entity."""
        self.delete_entities_batch([entity_id])

    def delete_entities_batch(self, entities):
        """Delete all series belonging to a batch of entities in a single heavy query."""
        if not entities:
            return
            
        names_to_drop = set()
        for ent in entities:
            names_to_drop.add(ent)
            obj_id = ent.split(".", 1)[1] if "." in ent else ent
            names_to_drop.add(obj_id)
            
        where_parts = [f"\"entity_id\" = '{name}'" for name in sorted(list(names_to_drop))]
        where_clause = " OR ".join(where_parts)
        
        import requests
        try:
            self.query(f"DROP SERIES WHERE {where_clause}", timeout=600)
        except requests.exceptions.Timeout:
            _LOGGER.warning(
                "[InfluxDB Cleanup] Der Loeschbefehl (DROP SERIES) hat das Zeitlimit ueberschritten. "
                "InfluxDB verarbeitet das Loeschen im Hintergrund weiter. Dies ist bei grossen Datenmengen normal."
            )
        except Exception as e:
            _LOGGER.warning("Batch DROP SERIES failed: %s", e)
            
        for name in sorted(list(names_to_drop)):
            try:
                self.query(f'DROP MEASUREMENT "{name}"', timeout=30)
            except requests.exceptions.Timeout:
                _LOGGER.warning(
                    "[InfluxDB Cleanup] DROP MEASUREMENT fuer %s hat das Zeitlimit ueberschritten. "
                    "Wird im InfluxDB-Hintergrund fortgesetzt.", name
                )
            except Exception:
                pass

    def check_entity_exists(self, entity_id):
        """Check if any series or measurement exists for the given entity ID."""
        obj_id = entity_id.split(".", 1)[1] if "." in entity_id else entity_id
        
        for name in [entity_id, obj_id]:
            try:
                res = self.query(f"SHOW SERIES WHERE \"entity_id\" = '{name}'")
                results = res.get("results", [])
                if results and "series" in results[0]:
                    return True
            except Exception:
                pass
                
        for name in [entity_id, obj_id]:
            try:
                res = self.query(f"SHOW MEASUREMENTS WITH MEASUREMENT = '{name}'")
                results = res.get("results", [])
                if results and "series" in results[0]:
                    return True
            except Exception:
                pass
                
        return False

    def discover_series_and_counts(self, old_entity):
        """Find measurements containing the entity and count total data points."""
        series_info = []
        total_points = 0
        has_timeout = False
        
        # 1. Construct a targeted list of candidate measurements
        possible_measurements = ["state", "°C", "%", "lx", "hPa", "km/h", "m/s", "mm", "min", "h"]
        possible_measurements.append(old_entity)
        if "." in old_entity:
            domain, object_id = old_entity.split(".", 1)
            possible_measurements.append(object_id)
            possible_measurements.append(domain)
        possible_measurements = sorted(list(set(possible_measurements)))
        _LOGGER.warning("[InfluxDB Validation] Target candidate measurements: %s", possible_measurements)

        # 2. Fetch all retention policies in the database (very fast)
        retention_policies = ["autogen"]
        try:
            rp_res = self.query("SHOW RETENTION POLICIES", timeout=10)
            results = rp_res.get("results", [])
            if results and "series" in results[0]:
                for val in results[0]["series"][0].get("values", []):
                    rp_name = val[0]
                    if rp_name not in retention_policies:
                        retention_policies.append(rp_name)
        except Exception as e:
            _LOGGER.warning("Could not fetch retention policies: %s", e)
        _LOGGER.warning("[InfluxDB Validation] Retention policies found/fallback: %s", retention_policies)

        # 3. Construct the list of fully qualified measurements to query in a single batch
        query_targets = []
        for m in possible_measurements:
            query_targets.append(f'"{m}"')
            for rp in retention_policies:
                if rp != "autogen":
                    query_targets.append(f'"{rp}"."{m}"')

        # 4. Query series using a single combined SHOW SERIES query
        measurements_found = set()
        resolved_tag = old_entity
        
        candidates = [old_entity]
        if "." in old_entity:
            candidates.append(old_entity.split(".", 1)[1])
            
        from_clause = ", ".join(query_targets)
        _LOGGER.warning("[InfluxDB Validation] Query targets from_clause: %s", from_clause)

        for entity_to_try in candidates:
            q = f"SHOW SERIES FROM {from_clause} WHERE \"entity_id\" = '{entity_to_try}'"
            _LOGGER.warning("[InfluxDB Validation] Running query: %s", q)
            try:
                result = self.query(q, timeout=20)
                _LOGGER.warning("[InfluxDB Validation] Query result: %s", result)
                results = result.get("results", [])
                if results and "series" in results[0]:
                    for series in results[0]["series"]:
                        for val in series.get("values", []):
                            series_str = val[0]
                            part0 = series_str.split(",")[0]
                            is_rp = False
                            if "." in part0:
                                parts = part0.split(".", 1)
                                rp = parts[0].strip('"')
                                if rp in retention_policies:
                                    is_rp = True
                                    m_name = parts[1].strip('"')
                                    quoted_name = f'"{rp}"."{m_name}"'
                            if not is_rp:
                                m_name = part0.strip('"')
                                quoted_name = f'"{m_name}"'
                            measurements_found.add(quoted_name)
                    resolved_tag = entity_to_try
                    break
            except Exception as e:
                _LOGGER.warning("Combined SHOW SERIES query failed for %s: %s", entity_to_try, e)

        # 3. For each measurement found, get count of points
        for measurement in sorted(list(measurements_found)):
            count = 0
            try:
                # Use a timeout of 15 seconds for count checks
                count_q = f"SELECT COUNT(*) FROM {measurement} WHERE \"entity_id\" = '{resolved_tag}'"
                _LOGGER.warning("[InfluxDB Validation] Running count query: %s", count_q)
                count_res = self.query(count_q, timeout=15)
                _LOGGER.warning("[InfluxDB Validation] Count query result: %s", count_res)
                res_results = count_res.get("results", [])
                if res_results and "series" in res_results[0]:
                    series_data = res_results[0]["series"][0]
                    if series_data.get("values") and len(series_data["values"][0]) > 1:
                        counts = []
                        for idx, val in enumerate(series_data["values"][0]):
                            if idx == 0:
                                continue
                            if val is not None:
                                try:
                                    counts.append(int(val))
                                except (ValueError, TypeError):
                                    pass
                        if counts:
                            count = max(counts)
            except Exception as e:
                _LOGGER.warning("Could not count InfluxDB points for %s in %s (timeout/error): %s", resolved_tag, measurement, e)
                count = -1
                has_timeout = True

            if count != 0:
                series_info.append({"measurement": measurement, "count": count})
                if count > 0 and not has_timeout:
                    total_points += count

        if has_timeout:
            total_points = -1

        return series_info, total_points, resolved_tag

    def migrate_entity_data(self, old_entity, new_entity, delete_old=False, progress_callback=None):
        """Read points of old_entity, update tag to new_entity, write back, and optionally delete old."""
        series_info, total_points, resolved_old_tag = self.discover_series_and_counts(old_entity)
        if total_points == 0:
            return {"status": "Success", "copied": 0, "deleted": 0}

        # Resolve target tag value
        # If the resolved old tag was stripped (didn't contain '.'), the new tag should be stripped too
        resolved_new_tag = new_entity
        if "." not in resolved_old_tag and "." in new_entity:
            resolved_new_tag = new_entity.split(".", 1)[1]

        copied_count = 0
        deleted_count = 0

        for s in series_info:
            measurement = s["measurement"]
            
            # 1. Discover Tag Keys and Field Keys to correctly parse them later
            tag_keys = set()
            tag_res = self.query(f"SHOW TAG KEYS FROM {measurement}")
            results = tag_res.get("results", [])
            if results and "series" in results[0]:
                for val in results[0]["series"][0].get("values", []):
                    tag_keys.add(val[0])
            
            field_keys = set()
            field_res = self.query(f"SHOW FIELD KEYS FROM {measurement}")
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
                q = f"SELECT * FROM {measurement} WHERE \"entity_id\" = '{resolved_old_tag}' LIMIT {chunk_size} OFFSET {offset}"
                data_res = self.query(q, timeout=120)
                
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
                            if col_name == "entity_id" and val_str == resolved_old_tag:
                                val_str = resolved_new_tag
                            
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
                        
                        # Escaped raw measurement name (without retention policy)
                        raw_meas = measurement
                        if "." in measurement:
                            parts = measurement.split(".", 1)
                            raw_meas = parts[1].strip('"')
                        else:
                            raw_meas = measurement.strip('"')
                        
                        meas_name = raw_meas.replace(" ", "\\ ").replace(",", "\\,")
                        
                        line = f"{meas_name}{tag_str} {field_str} {ns_timestamp}"
                        lines_to_write.append(line)

                if lines_to_write:
                    self.write_lines(lines_to_write)
                    import time
                    time.sleep(0.25)  # Increased throttle to prevent InfluxDB WAL/TSM engine crashes
                    copied_count += len(lines_to_write)
                    if progress_callback:
                        progress_callback(copied_count, total_points)
                    if copied_count % 25000 == 0:
                        _LOGGER.warning("[InfluxDB Migration] %d Punkte erreicht. Pausiere für 2 Sekunden, damit InfluxDB offene WAL-Dateien schließen kann...", copied_count)
                        time.sleep(2.0)

                if len(values) < chunk_size:
                    break
                offset += chunk_size

            # 3. Optionally delete the old data from this measurement
            if delete_old and copied_count > 0:
                del_q = f"DELETE FROM {measurement} WHERE \"entity_id\" = '{resolved_old_tag}'"
                self.query(del_q)
                deleted_count += s["count"]

        return {
            "status": "Success",
            "copied": copied_count,
            "deleted": deleted_count
        }
