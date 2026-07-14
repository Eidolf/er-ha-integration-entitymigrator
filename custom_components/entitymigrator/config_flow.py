"""Config flow for Entity Statistics Migrator integration."""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

import voluptuous as vol
from sqlalchemy import text

from homeassistant import config_entries
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import list_statistic_ids
from homeassistant.core import HomeAssistant, callback
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import (
    device_registry as dr,
    entity_registry as er,
    selector,
)
import homeassistant.util.dt as dt_util

from .const import (
    CONF_CUTOFF_DATE,
    CONF_DELETE_OLD,
    CONF_NEW_ENTITY_ID,
    CONF_OLD_ENTITY_ID,
    CONF_MODE,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)


def run_db_migration(
    hass: HomeAssistant,
    mappings: list[tuple[str, str]],
    cutoff_date_str: str,
    delete_old: bool,
    influx_config: dict[str, Any] | None = None,
    influxdb_only: bool = False,
) -> dict[str, Any]:
    """Run the database migration transaction in the executor thread."""
    dt = dt_util.parse_datetime(cutoff_date_str)
    if dt is None:
        raise ValueError("Invalid datetime format")
    dt = dt_util.as_utc(dt)
    ts = dt.timestamp()

    result_summary = {
        "status": "Erfolgreich",
        "migration_type": "Langzeitstatistiken (LTS)",
        "deleted": "Nein",
        "details": [],
    }

    if influxdb_only:
        if influx_config:
            try:
                from .influx_migrator import InfluxV1Migrator
                with InfluxV1Migrator(
                    host=influx_config["host"],
                    port=influx_config["port"],
                    database=influx_config["database"],
                    username=influx_config.get("username"),
                    password=influx_config.get("password"),
                    ssl=influx_config.get("ssl", False),
                ) as migrator:
                    def make_progress_callback(old_ent, new_ent):
                        def progress_cb(copied, total):
                            if total > 0:
                                pct = (copied / total) * 100
                                _LOGGER.info(
                                    "[InfluxDB Migration] '%s' -> '%s': %d / %d Punkte kopiert (%.1f%%)",
                                    old_ent, new_ent, copied, total, pct
                                )
                            else:
                                _LOGGER.info(
                                    "[InfluxDB Migration] '%s' -> '%s': %d Punkte kopiert",
                                    old_ent, new_ent, copied
                                )
                        return progress_cb

                    for old_entity, new_entity in mappings:
                        _LOGGER.info("[InfluxDB Migration] Starte Kopiervorgang für '%s' -> '%s'...", old_entity, new_entity)
                        res = migrator.migrate_entity_data(
                            old_entity=old_entity,
                            new_entity=new_entity,
                            delete_old=delete_old,
                            progress_callback=make_progress_callback(old_entity, new_entity)
                        )
                        copied = res["copied"]
                        deleted = res["deleted"]
                        _LOGGER.info("[InfluxDB Migration] '%s' -> '%s' erfolgreich abgeschlossen: %d Punkte kopiert", old_entity, new_entity, copied)
                        result_summary["details"].append(
                            f"InfluxDB '{old_entity}' -> '{new_entity}': {copied} Datenpunkte kopiert"
                            + (f", {deleted} alte Datenpunkte gelöscht" if delete_old else "")
                        )
                result_summary["migration_type"] = "Nur InfluxDB"
            except Exception as e:
                _LOGGER.error("Fehler bei der InfluxDB-Migration: %s", e)
                result_summary["details"].append(f"InfluxDB-Fehler: {e}")
        return result_summary

    session = get_instance(hass).get_session()
    try:
        # 1. Determine active time column (start_ts or start)
        test_query = session.execute(text("SELECT * FROM statistics LIMIT 1"))
        has_start_ts = "start_ts" in test_query.keys()
        time_col = "start_ts" if has_start_ts else "start"
        time_val = ts if has_start_ts else dt

        has_lts_any = False
        has_states_any = False

        max_attempts = 10
        success = False
        last_err = None

        for attempt in range(max_attempts):
            try:
                session.rollback()
                if session.bind.dialect.name == "sqlite":
                    session.execute(text("PRAGMA busy_timeout = 30000"))
                    session.execute(text("BEGIN IMMEDIATE"))

                result_summary["details"] = []
                has_lts_any = False
                has_states_any = False

                _LOGGER.info("[SQL Migration] Starte SQL-Migration für %d Entitäten...", len(mappings))
                for old_entity, new_entity in mappings:
                    if not old_entity or not new_entity:
                        continue
                    _LOGGER.info("[SQL Migration] Verarbeite '%s' -> '%s'...", old_entity, new_entity)

                    # 2. Cleanup old states history if requested
                    if delete_old:
                        states_query = session.execute(text("SELECT * FROM states LIMIT 1"))
                        has_states_metadata = "metadata_id" in states_query.keys()
                        has_entity_id = "entity_id" in states_query.keys()

                        if has_states_metadata:
                            states_meta = session.execute(
                                text("SELECT metadata_id FROM states_meta WHERE entity_id = :old"),
                                {"old": old_entity}
                            ).fetchone()
                            if states_meta:
                                states_meta_id = states_meta[0]
                                session.execute(
                                    text("DELETE FROM states WHERE metadata_id = :meta_id"),
                                    {"meta_id": states_meta_id}
                                )
                                session.execute(
                                    text("DELETE FROM states_meta WHERE metadata_id = :meta_id"),
                                    {"meta_id": states_meta_id}
                                )
                                has_states_any = True
                        
                        if has_entity_id:
                            session.execute(
                                text("DELETE FROM states WHERE entity_id = :old"),
                                {"old": old_entity}
                            )
                            has_states_any = True
                        
                        result_summary["deleted"] = "Ja"

                    # 3. Get old metadata
                    old_meta = session.execute(
                        text("SELECT id, has_sum, source, unit_of_measurement, has_mean FROM statistics_meta WHERE statistic_id = :old"),
                        {"old": old_entity}
                    ).fetchone()

                    if not old_meta:
                        _LOGGER.warning("Old entity %s has no statistics metadata; skipping LTS migration.", old_entity)
                        result_summary["details"].append(f"{old_entity} -> {new_entity}: Keine LTS vorhanden")
                        continue

                    old_meta_id, has_sum, source, old_unit, has_mean = old_meta
                    has_lts_any = True

                    # 4. Get or create new metadata
                    new_meta = session.execute(
                        text("SELECT id, unit_of_measurement FROM statistics_meta WHERE statistic_id = :new"),
                        {"new": new_entity}
                    ).fetchone()

                    new_unit = None
                    if new_meta:
                        new_meta_id, new_unit = new_meta
                    else:
                        new_state = hass.states.get(new_entity)
                        if new_state:
                            new_unit = new_state.attributes.get("unit_of_measurement")

                    # Validate unit matching and scale factor
                    scale_factor = 1.0
                    if old_unit and new_unit:
                        old_u_norm = old_unit.strip().lower()
                        new_u_norm = new_unit.strip().lower()
                        if old_u_norm != new_u_norm:
                            if old_u_norm == "kwh" and new_u_norm == "wh":
                                scale_factor = 1000.0
                            elif old_u_norm == "wh" and new_u_norm == "kwh":
                                scale_factor = 0.001
                            elif old_u_norm == "mwh" and new_u_norm == "wh":
                                scale_factor = 1000000.0
                            elif old_u_norm == "wh" and new_u_norm == "mwh":
                                scale_factor = 0.000001
                            elif old_u_norm == "mwh" and new_u_norm == "kwh":
                                scale_factor = 1000.0
                            elif old_u_norm == "kwh" and new_u_norm == "mwh":
                                scale_factor = 0.001
                            else:
                                raise ValueError(f"UNIT_MISMATCH:{old_entity}:{new_entity}:{old_unit}:{new_unit}")

                    if not new_meta:
                        session.execute(
                            text(
                                "INSERT INTO statistics_meta (statistic_id, source, unit_of_measurement, has_mean, has_sum) "
                                "VALUES (:new, :source, :unit, :has_mean, :has_sum)"
                            ),
                            {
                                "new": new_entity,
                                "source": source or "recorder",
                                "unit": old_unit,
                                "has_mean": has_mean,
                                "has_sum": has_sum,
                            }
                        )
                        new_meta = session.execute(
                            text("SELECT id FROM statistics_meta WHERE statistic_id = :new"),
                            {"new": new_entity}
                        ).fetchone()

                    new_meta_id = new_meta[0]

                    # 5. Unique-Constraint-Bereinigung
                    session.execute(
                        text(f"DELETE FROM statistics WHERE metadata_id = :new_id AND {time_col} < :time_val"),
                        {"new_id": new_meta_id, "time_val": time_val}
                    )
                    session.execute(
                        text(f"DELETE FROM statistics_short_term WHERE metadata_id = :new_id AND {time_col} < :time_val"),
                        {"new_id": new_meta_id, "time_val": time_val}
                    )

                    # 6. Offset-Berechnung
                    if has_sum:
                        old_sum_row = session.execute(
                            text(f"SELECT sum FROM statistics WHERE metadata_id = :old_id AND {time_col} < :time_val ORDER BY {time_col} DESC LIMIT 1"),
                            {"old_id": old_meta_id, "time_val": time_val}
                        ).fetchone()

                        if old_sum_row is None or old_sum_row[0] is None:
                            old_sum_row = session.execute(
                                text(f"SELECT sum FROM statistics WHERE metadata_id = :old_id ORDER BY {time_col} ASC LIMIT 1"),
                                {"old_id": old_meta_id}
                            ).fetchone()

                        new_sum_row = session.execute(
                            text(f"SELECT sum FROM statistics WHERE metadata_id = :new_id AND {time_col} >= :time_val ORDER BY {time_col} ASC LIMIT 1"),
                            {"new_id": new_meta_id, "time_val": time_val}
                        ).fetchone()

                        if new_sum_row is None or new_sum_row[0] is None:
                            new_sum_row = session.execute(
                                text(f"SELECT sum FROM statistics WHERE metadata_id = :new_id ORDER BY {time_col} ASC LIMIT 1"),
                                {"new_id": new_meta_id}
                            ).fetchone()

                        if new_sum_row is None or new_sum_row[0] is None:
                            new_state = hass.states.get(new_entity)
                            if new_state is not None:
                                try:
                                    val = float(new_state.state)
                                    new_sum_row = (val,)
                                except (ValueError, TypeError):
                                    pass

                        if old_sum_row is not None and old_sum_row[0] is not None and new_sum_row is not None and new_sum_row[0] is not None:
                            old_sum = (old_sum_row[0] or 0.0) * scale_factor
                            new_sum = new_sum_row[0] or 0.0
                            offset = old_sum - new_sum
                            if offset != 0:
                                session.execute(
                                    text(f"UPDATE statistics SET sum = sum + :offset WHERE metadata_id = :new_id AND {time_col} >= :time_val"),
                                    {"offset": offset, "new_id": new_meta_id, "time_val": time_val}
                                )
                                session.execute(
                                    text(f"UPDATE statistics_short_term SET sum = sum + :offset WHERE metadata_id = :new_id AND {time_col} >= :time_val"),
                                    {"offset": offset, "new_id": new_meta_id, "time_val": time_val}
                                )

                    # 7. Perform migration (UPDATE)
                    if scale_factor != 1.0:
                        session.execute(
                            text(
                                f"UPDATE statistics SET "
                                f"metadata_id = :new_id, "
                                f"sum = sum * :factor, "
                                f"state = state * :factor, "
                                f"min = min * :factor, "
                                f"max = max * :factor, "
                                f"mean = mean * :factor "
                                f"WHERE metadata_id = :old_id AND {time_col} < :time_val"
                            ),
                            {"new_id": new_meta_id, "old_id": old_meta_id, "time_val": time_val, "factor": scale_factor}
                        )
                        session.execute(
                            text(
                                f"UPDATE statistics_short_term SET "
                                f"metadata_id = :new_id, "
                                f"sum = sum * :factor, "
                                f"state = state * :factor, "
                                f"min = min * :factor, "
                                f"max = max * :factor, "
                                f"mean = mean * :factor "
                                f"WHERE metadata_id = :old_id AND {time_col} < :time_val"
                            ),
                            {"new_id": new_meta_id, "old_id": old_meta_id, "time_val": time_val, "factor": scale_factor}
                        )
                    else:
                        session.execute(
                            text(f"UPDATE statistics SET metadata_id = :new_id WHERE metadata_id = :old_id AND {time_col} < :time_val"),
                            {"new_id": new_meta_id, "old_id": old_meta_id, "time_val": time_val}
                        )
                        session.execute(
                            text(f"UPDATE statistics_short_term SET metadata_id = :new_id WHERE metadata_id = :old_id AND {time_col} < :time_val"),
                            {"new_id": new_meta_id, "old_id": old_meta_id, "time_val": time_val}
                        )

                    # 8. Cleanup old statistics if requested
                    if delete_old:
                        session.execute(
                            text("DELETE FROM statistics WHERE metadata_id = :old_id"),
                            {"old_id": old_meta_id}
                        )
                        session.execute(
                            text("DELETE FROM statistics_short_term WHERE metadata_id = :old_id"),
                            {"old_id": old_meta_id}
                        )
                        session.execute(
                            text("DELETE FROM statistics_meta WHERE id = :old_id"),
                            {"old_id": old_meta_id}
                        )

                    _LOGGER.info("[SQL Migration] '%s' -> '%s' erfolgreich migriert.", old_entity, new_entity)
                    result_summary["details"].append(f"{old_entity} -> {new_entity}: Erfolgreich")

                session.commit()
                session.close()
                success = True
                break
            except ValueError as err:
                session.rollback()
                raise err
            except Exception as err:
                session.rollback()
                err_str = str(err).lower()
                if "locked" in err_str or "busy" in err_str or "database is locked" in err_str:
                    import random
                    sleep_time = random.uniform(2.0, 5.0)
                    _LOGGER.warning(
                        "Database locked during migration. Retrying in %.1f seconds... (Attempt %s/%s)",
                        sleep_time, attempt + 1, max_attempts
                    )
                    time.sleep(sleep_time)
                    last_err = err
                    continue
                raise err

        if not success:
            _LOGGER.error("Failed to migrate mappings after %s attempts due to database lock.", max_attempts)
            raise last_err

        if not has_lts_any:
            result_summary["status"] = "Erfolgreich (keine LTS vorhanden)"
            result_summary["migration_type"] = "Nur Kurzzeit-Zustände (Zustandsverlauf)"
        elif has_states_any:
            result_summary["migration_type"] = "LTS & Kurzzeit-Zustände"

        # 9. Optionally perform InfluxDB migration
        if influx_config and success:
            try:
                from .influx_migrator import InfluxV1Migrator
                with InfluxV1Migrator(
                    host=influx_config["host"],
                    port=influx_config["port"],
                    database=influx_config["database"],
                    username=influx_config.get("username"),
                    password=influx_config.get("password"),
                    ssl=influx_config.get("ssl", False),
                ) as migrator:
                    def make_progress_callback(old_ent, new_ent):
                        def progress_cb(copied, total):
                            if total > 0:
                                pct = (copied / total) * 100
                                _LOGGER.info(
                                    "[InfluxDB Migration] '%s' -> '%s': %d / %d Punkte kopiert (%.1f%%)",
                                    old_ent, new_ent, copied, total, pct
                                )
                            else:
                                _LOGGER.info(
                                    "[InfluxDB Migration] '%s' -> '%s': %d Punkte kopiert",
                                    old_ent, new_ent, copied
                                )
                        return progress_cb

                    for old_entity, new_entity in mappings:
                        _LOGGER.info("[InfluxDB Migration] Starte Kopiervorgang für '%s' -> '%s'...", old_entity, new_entity)
                        res = migrator.migrate_entity_data(
                            old_entity=old_entity,
                            new_entity=new_entity,
                            delete_old=delete_old,
                            progress_callback=make_progress_callback(old_entity, new_entity)
                        )
                        copied = res["copied"]
                        deleted = res["deleted"]
                        _LOGGER.info("[InfluxDB Migration] '%s' -> '%s' erfolgreich abgeschlossen: %d Punkte kopiert", old_entity, new_entity, copied)
                        result_summary["details"].append(
                            f"InfluxDB '{old_entity}' -> '{new_entity}': {copied} Datenpunkte kopiert"
                            + (f", {deleted} alte Datenpunkte gelöscht" if delete_old else "")
                        )
                result_summary["migration_type"] = f"{result_summary['migration_type']} & InfluxDB"
            except Exception as e:
                _LOGGER.error("Fehler bei der InfluxDB-Migration: %s", e)
                result_summary["details"].append(f"InfluxDB-Fehler: {e}")

        return result_summary
    except Exception as err:
        try:
            session.rollback()
        except Exception:
            pass
        _LOGGER.error("Database migration error: %s", err)
        raise err
    finally:
        try:
            session.close()
        except Exception:
            pass
def check_migration_warnings(
    hass: HomeAssistant,
    mappings: list[tuple[str, str]],
    cutoff_date_str: str,
    influx_config: dict[str, Any] | None = None,
    influxdb_only: bool = False,
) -> list[str]:
    """Validate mappings before migrating to prevent accidental data loss/overwrite."""
    dt = dt_util.parse_datetime(cutoff_date_str)
    if dt is None:
        return []
    dt = dt_util.as_utc(dt)
    ts = dt.timestamp()

    warnings = []

    if not influxdb_only:
        session = get_instance(hass).get_session()
        try:
            test_query = session.execute(text("SELECT * FROM statistics LIMIT 1"))
            has_start_ts = "start_ts" in test_query.keys()
            time_col = "start_ts" if has_start_ts else "start"
            time_val = ts if has_start_ts else dt

            for old_entity, new_entity in mappings:
                # 1. Check if old_entity statistics exist or count is 0
                old_meta = session.execute(
                    text("SELECT id FROM statistics_meta WHERE statistic_id = :old"),
                    {"old": old_entity}
                ).fetchone()
                
                if not old_meta:
                    warnings.append(
                        f"Die Quell-Entität '{old_entity}' hat keine Statistiken in der Datenbank. "
                        "Sie wurde eventuell bereits migriert."
                    )
                else:
                    old_meta_id = old_meta[0]
                    count_row = session.execute(
                        text(f"SELECT COUNT(*) FROM statistics WHERE metadata_id = :old_id"),
                        {"old_id": old_meta_id}
                    ).fetchone()
                    if not count_row or count_row[0] == 0:
                        warnings.append(
                            f"Die Quell-Entität '{old_entity}' hat 0 Statistik-Einträge in der Datenbank. "
                            "Sie wurde eventuell bereits migriert."
                        )

                # 2. Check if new_entity already has statistics prior to cutoff (potential overwrite)
                new_meta = session.execute(
                    text("SELECT id FROM statistics_meta WHERE statistic_id = :new"),
                    {"new": new_entity}
                ).fetchone()
                if new_meta:
                    new_meta_id = new_meta[0]
                    has_existing = session.execute(
                        text(f"SELECT COUNT(*) FROM statistics WHERE metadata_id = :new_id AND {time_col} < :time_val"),
                        {"new_id": new_meta_id, "time_val": time_val}
                    ).fetchone()
                    if has_existing and has_existing[0] > 0:
                        warnings.append(
                            f"Die Ziel-Entität '{new_entity}' hat bereits {has_existing[0]} eigene Langzeitstatistiken "
                            "vor dem Cutoff-Datum. Diese werden bei der Migration überschrieben!"
                        )
        except Exception as e:
            _LOGGER.error("Fehler bei der Validierung der Migration (SQL): %s", e)
        finally:
            try:
                session.close()
            except Exception:
                pass

    if influx_config:
        try:
            from .influx_migrator import InfluxV1Migrator
            with InfluxV1Migrator(
                host=influx_config["host"],
                port=influx_config["port"],
                database=influx_config["database"],
                username=influx_config.get("username"),
                password=influx_config.get("password"),
                ssl=influx_config.get("ssl", False)
            ) as migrator:
                for old_entity, new_entity in mappings:
                    series_info, total_points, _ = migrator.discover_series_and_counts(old_entity)
                    if total_points > 0:
                        warnings.append(
                            f"InfluxDB: Für '{old_entity}' wurden {total_points} historische Datenpunkte in "
                            f"{len(series_info)} Measurements gefunden. Diese werden in '{new_entity}' kopiert."
                        )
                    elif total_points == -1:
                        warnings.append(
                            f"InfluxDB: Für '{old_entity}' wurden historische Datenpunkte in "
                            f"{len(series_info)} Measurements gefunden (genaue Anzahl konnte wegen InfluxDB-Timeout nicht ermittelt werden). Diese werden kopiert."
                        )
                    else:
                        warnings.append(
                            f"InfluxDB: Für '{old_entity}' wurden keine historischen Datenpunkte gefunden."
                        )
        except Exception as e:
            warnings.append(f"InfluxDB-Fehler bei der Überprüfung: {e}")
    return warnings


def run_migration_in_background(
    hass: HomeAssistant,
    mappings: list[tuple[str, str]],
    cutoff_date_str: str,
    delete_old: bool,
    influx_config: dict[str, Any] | None = None,
    influxdb_only: bool = False,
) -> None:
    """Run the database migration transaction asynchronously in a background thread."""
    def _run():
        try:
            summary = run_db_migration(
                hass,
                mappings,
                cutoff_date_str,
                delete_old,
                influx_config,
                influxdb_only
            )
            # Create persistent notification on success
            details = "\n".join([f"- {d}" for d in summary.get("details", [])])
            from homeassistant.components import persistent_notification
            persistent_notification.create(
                hass,
                title="Statistik-Migration abgeschlossen",
                message=(
                    f"Die Migration für {len(mappings)} Entitäten wurde erfolgreich abgeschlossen!\n\n"
                    f"**Status**: {summary.get('status')}\n"
                    f"**Typ**: {summary.get('migration_type')}\n"
                    f"**Details**:\n{details}"
                ),
                notification_id="entitymigrator_migration"
            )
        except Exception as err:
            _LOGGER.error("Migration failed in background: %s", err)
            from homeassistant.components import persistent_notification
            persistent_notification.create(
                hass,
                title="Statistik-Migration fehlgeschlagen",
                message=f"Während der Migration ist ein Fehler aufgetreten: {err}",
                notification_id="entitymigrator_migration"
            )

    hass.async_add_executor_job(_run)



class EntityMigratorConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Entity Statistics Migrator."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial setup mode step."""
        errors = {}

        if user_input is not None:
            # Save configuration settings globally in flow context
            self.context["mode"] = user_input[CONF_MODE]
            self.context["cutoff_date"] = user_input[CONF_CUTOFF_DATE]
            self.context["delete_old"] = user_input.get(CONF_DELETE_OLD, False)
            self.context["influxdb_migrate"] = user_input.get("influxdb_migrate", False)
            self.context["influxdb_only"] = user_input.get("influxdb_only", False)
            self.context["mappings"] = []

            # If influxdb_only is True, force influxdb_migrate to be True
            if self.context["influxdb_only"]:
                self.context["influxdb_migrate"] = True

            if self.context["influxdb_migrate"]:
                return await self.async_step_influxdb()

            if user_input[CONF_MODE] == "device":
                return await self.async_step_device()
            elif user_input[CONF_MODE] == "loop":
                return await self.async_step_loop()
            else:
                # Default/Single
                return await self.async_step_loop()

        schema = vol.Schema(
            {
                vol.Required(CONF_MODE, default="single"): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=[
                            selector.SelectOptionDict(
                                value="single", label="Einzelne Entität migrieren"
                            ),
                            selector.SelectOptionDict(
                                value="device",
                                label="Mehrere Entitäten eines Gerätes migrieren",
                            ),
                            selector.SelectOptionDict(
                                value="loop",
                                label="Mehrere Entitäten manuell nacheinander hinzufügen (Loop)",
                            ),
                        ],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                ),
                vol.Required(CONF_CUTOFF_DATE): selector.DateTimeSelector(),
                vol.Optional(CONF_DELETE_OLD, default=False): selector.BooleanSelector(),
                vol.Optional("influxdb_migrate", default=False): selector.BooleanSelector(),
                vol.Optional("influxdb_only", default=False): selector.BooleanSelector(),
            }
        )

        return self.async_show_form(
            step_id="user", data_schema=schema, errors=errors
        )
    async def async_step_influxdb(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle InfluxDB v1 configuration inputs."""
        errors = {}

        if user_input is not None:
            self.context["influx_config"] = {
                "host": user_input["influx_host"],
                "port": user_input["influx_port"],
                "database": user_input["influx_database"],
                "username": user_input.get("influx_username"),
                "password": user_input.get("influx_password"),
                "ssl": user_input.get("influx_ssl", False),
            }

            try:
                from .influx_migrator import InfluxV1Migrator
                with InfluxV1Migrator(
                    host=user_input["influx_host"],
                    port=user_input["influx_port"],
                    database=user_input["influx_database"],
                    username=user_input.get("influx_username"),
                    password=user_input.get("influx_password"),
                    ssl=user_input.get("influx_ssl", False)
                ) as migrator:
                    await self.hass.async_add_executor_job(migrator.test_connection)
                
                if self.context["mode"] == "device":
                    return await self.async_step_device()
                else:
                    return await self.async_step_loop()
            except Exception as e:
                _LOGGER.error("InfluxDB connection test failed: %s", e)
                errors["base"] = "influx_conn_error"

        schema = vol.Schema(
            {
                vol.Required("influx_host", default="localhost"): str,
                vol.Required("influx_port", default=8086): int,
                vol.Required("influx_database", default="homeassistant"): str,
                vol.Optional("influx_username"): str,
                vol.Optional("influx_password"): str,
                vol.Optional("influx_ssl", default=False): selector.BooleanSelector(),
            }
        )

        return self.async_show_form(
            step_id="influxdb", data_schema=schema, errors=errors
        )


    async def async_step_device(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle selecting the target Device."""
        errors = {}

        if user_input is not None:
            self.context["device_id"] = user_input["device_id"]
            return await self.async_step_device_map()

        device_registry = dr.async_get(self.hass)
        devices = device_registry.devices
        device_options = []
        for dev_id, dev_entry in devices.items():
            name = dev_entry.name_by_user or dev_entry.name or dev_id
            device_options.append(selector.SelectOptionDict(value=dev_id, label=name))

        if not device_options:
            device_options = [
                selector.SelectOptionDict(
                    value="none", label="Keine Geräte im System vorhanden"
                )
            ]

        schema = vol.Schema(
            {
                vol.Required("device_id"): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=device_options,
                        mode=selector.SelectSelectorMode.DROPDOWN,
                        custom_value=True,
                    )
                )
            }
        )

        return self.async_show_form(
            step_id="device", data_schema=schema, errors=errors
        )

    async def async_step_device_map(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle mapping old entities to device entities."""
        errors = {}
        device_id = self.context.get("device_id")

        # Get entities belonging to the selected device
        entity_registry = er.async_get(self.hass)
        device_entities = er.async_entries_for_device(entity_registry, device_id)

        if not device_entities:
            errors["base"] = "no_device_entities"
            # Fallback to device step
            return await self.async_step_device()

        # Retrieve entities with statistics to build dropdown list
        try:
            stats = await get_instance(self.hass).async_add_executor_job(
                list_statistic_ids, self.hass
            )
            old_entities = {
                item["statistic_id"] for item in stats if "statistic_id" in item
            }
        except Exception as err:
            _LOGGER.error("Failed to list statistic IDs: %s", err)
            old_entities = set()

        all_entities = set(self.hass.states.async_entity_ids())
        all_entities.update(old_entities)

        options_list = []
        for entity_id in sorted(list(all_entities)):
            state = self.hass.states.get(entity_id)
            val_info = ""
            if state:
                val = state.state
                unit = state.attributes.get("unit_of_measurement", "")
                val_info = f"[{val} {unit}] "
            options_list.append(
                selector.SelectOptionDict(
                    value=entity_id,
                    label=f"{val_info}{entity_id}"
                )
            )

        if user_input is not None:
            mappings = []
            for entry in device_entities:
                key_base = entry.entity_id.replace(".", "__")
                matched_key = None
                for k in user_input:
                    if k.endswith(key_base):
                        matched_key = k
                        break
                if matched_key:
                    old_entity = user_input.get(matched_key)
                    if old_entity:
                        mappings.append((old_entity, entry.entity_id))

            if not mappings:
                errors["base"] = "no_device_entities"
            else:
                try:
                    warnings = await get_instance(self.hass).async_add_executor_job(
                        check_migration_warnings,
                        self.hass,
                        mappings,
                        self.context["cutoff_date"],
                        self.context.get("influx_config"),
                        self.context.get("influxdb_only", False),
                    )

                    self.context["mappings"] = mappings

                    device_registry = dr.async_get(self.hass)
                    device_entry = device_registry.async_get(device_id)
                    device_name = device_id
                    if device_entry:
                        device_name = device_entry.name_by_user or device_entry.name or device_id

                    self.context["init_data"] = {
                        CONF_OLD_ENTITY_ID: f"Device Migration ({len(mappings)} Entitäten)",
                        CONF_NEW_ENTITY_ID: device_name,
                    }

                    if warnings:
                        self.context["migration_warnings"] = warnings
                        return await self.async_step_confirm()

                    run_migration_in_background(
                        self.hass,
                        mappings,
                        self.context["cutoff_date"],
                        self.context["delete_old"],
                        self.context.get("influx_config"),
                        self.context.get("influxdb_only", False),
                    )
                    self.context["migration_result"] = {
                        "status": "Hintergrund-Migration gestartet",
                        "migration_type": "Asynchroner Task",
                        "deleted": "Nein",
                        "details": [
                            "Die Migration wurde im Hintergrund gestartet, um Timeouts zu verhindern.",
                            "Home Assistant benachrichtigt dich über das Glocken-Symbol unten links,",
                            "sobald der Vorgang vollständig abgeschlossen ist."
                        ]
                    }
                    return await self.async_step_summary()
                except ValueError as err:
                    if str(err).startswith("UNIT_MISMATCH:"):
                        parts = str(err).split(":")
                        bad_new_entity = parts[2]
                        bad_key_base = bad_new_entity.replace(".", "__")
                        
                        found_key = None
                        for entry in device_entities:
                            if entry.entity_id == bad_new_entity:
                                new_state = self.hass.states.get(entry.entity_id)
                                val_info = ""
                                if new_state:
                                    val = new_state.state
                                    unit = new_state.attributes.get("unit_of_measurement", "")
                                    val_info = f"[{val} {unit}] "
                                found_key = f"{val_info}{bad_key_base}"
                                break
                        
                        if found_key:
                            errors[found_key] = "unit_mismatch"
                        else:
                            errors["base"] = "unit_mismatch"
                    else:
                        errors["base"] = "db_error"
                except Exception:
                    errors["base"] = "db_error"

        # Build schema dynamically for each entity of the device
        schema_fields = {}
        for entry in device_entities:
            key_base = entry.entity_id.replace(".", "__")
            new_state = self.hass.states.get(entry.entity_id)
            val_info = ""
            if new_state:
                val = new_state.state
                unit = new_state.attributes.get("unit_of_measurement", "")
                val_info = f"[{val} {unit}] "
            
            key = f"{val_info}{key_base}"
            schema_fields[
                vol.Optional(key, description={"suggested_value": ""})
            ] = selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=options_list,
                    mode=selector.SelectSelectorMode.DROPDOWN,
                    custom_value=True,
                )
            )

        return self.async_show_form(
            step_id="device_map",
            data_schema=vol.Schema(schema_fields),
            errors=errors,
        )

    async def async_step_loop(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle mapping entities manually in a loop."""
        errors = {}

        # Retrieve entities with statistics
        try:
            stats = await get_instance(self.hass).async_add_executor_job(
                list_statistic_ids, self.hass
            )
            old_entities = {
                item["statistic_id"] for item in stats if "statistic_id" in item
            }
        except Exception as err:
            _LOGGER.error("Failed to list statistic IDs: %s", err)
            old_entities = set()

        all_entities = set(self.hass.states.async_entity_ids())
        all_entities.update(old_entities)

        options_list = []
        for entity_id in sorted(list(all_entities)):
            state = self.hass.states.get(entity_id)
            val_info = ""
            if state:
                val = state.state
                unit = state.attributes.get("unit_of_measurement", "")
                val_info = f"[{val} {unit}] "
            options_list.append(
                selector.SelectOptionDict(
                    value=entity_id,
                    label=f"{val_info}{entity_id}"
                )
            )

        if user_input is not None:
            old_entity = user_input[CONF_OLD_ENTITY_ID]
            new_entity = user_input[CONF_NEW_ENTITY_ID]
            another = user_input.get("another", False)

            if old_entity == new_entity:
                errors["base"] = "same_entity"
            else:
                self.context["mappings"].append((old_entity, new_entity))

                if another:
                    # Clear and loop again
                    return await self.async_step_loop()

                # Process all mappings
                try:
                    warnings = await get_instance(self.hass).async_add_executor_job(
                        check_migration_warnings,
                        self.hass,
                        self.context["mappings"],
                        self.context["cutoff_date"],
                        self.context.get("influx_config"),
                        self.context.get("influxdb_only", False),
                    )

                    self.context["init_data"] = {
                        CONF_OLD_ENTITY_ID: f"Migration ({len(self.context['mappings'])} Entitäten)",
                        CONF_NEW_ENTITY_ID: f"{len(self.context['mappings'])} Ziele",
                    }

                    if warnings:
                        self.context["migration_warnings"] = warnings
                        return await self.async_step_confirm()

                    run_migration_in_background(
                        self.hass,
                        self.context["mappings"],
                        self.context["cutoff_date"],
                        self.context["delete_old"],
                        self.context.get("influx_config"),
                        self.context.get("influxdb_only", False),
                    )
                    self.context["migration_result"] = {
                        "status": "Hintergrund-Migration gestartet",
                        "migration_type": "Asynchroner Task",
                        "deleted": "Nein",
                        "details": [
                            "Die Migration wurde im Hintergrund gestartet, um Timeouts zu verhindern.",
                            "Home Assistant benachrichtigt dich über das Glocken-Symbol unten links,",
                            "sobald der Vorgang vollständig abgeschlossen ist."
                        ]
                    }
                    return await self.async_step_summary()
                except ValueError as err:
                    if str(err).startswith("UNIT_MISMATCH:"):
                        errors[CONF_NEW_ENTITY_ID] = "unit_mismatch"
                    else:
                        errors["base"] = "db_error"
                except Exception:
                    errors["base"] = "db_error"

        schema = vol.Schema(
            {
                vol.Required(CONF_OLD_ENTITY_ID): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=options_list,
                        mode=selector.SelectSelectorMode.DROPDOWN,
                        custom_value=True,
                    )
                ),
                vol.Required(CONF_NEW_ENTITY_ID): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=options_list,
                        mode=selector.SelectSelectorMode.DROPDOWN,
                        custom_value=True,
                    )
                ),
                vol.Optional("another", default=False): selector.BooleanSelector(),
            }
        )

        # Show loop count in title if more than 0
        loop_title = "Manuelle Migration"
        if self.context.get("mappings"):
            loop_title += f" (Eintrag #{len(self.context['mappings']) + 1})"

        return self.async_show_form(
            step_id="loop",
            data_schema=schema,
            errors=errors,
        )

    async def async_step_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle confirmation of warnings before executing migration."""
        errors = {}
        warnings = self.context.get("migration_warnings", [])

        if user_input is not None:
            if user_input.get("confirm"):
                try:
                    run_migration_in_background(
                        self.hass,
                        self.context["mappings"],
                        self.context["cutoff_date"],
                        self.context["delete_old"],
                        self.context.get("influx_config"),
                        self.context.get("influxdb_only", False),
                    )
                    self.context["migration_result"] = {
                        "status": "Hintergrund-Migration gestartet",
                        "migration_type": "Asynchroner Task",
                        "deleted": "Nein",
                        "details": [
                            "Die Migration wurde im Hintergrund gestartet, um Timeouts zu verhindern.",
                            "Home Assistant benachrichtigt dich über das Glocken-Symbol unten links,",
                            "sobald der Vorgang vollständig abgeschlossen ist."
                        ]
                    }
                    return await self.async_step_summary()
                except ValueError as err:
                    if str(err).startswith("UNIT_MISMATCH:"):
                        errors["base"] = "unit_mismatch"
                    else:
                        errors["base"] = "db_error"
                except Exception:
                    errors["base"] = "db_error"
            else:
                errors["base"] = "not_confirmed"

        warnings_text = "\n\n".join([f"⚠️ {w}" for w in warnings])
        warnings_text = warnings_text.replace("%", "%%")
        description_placeholders = {"warnings_text": warnings_text}

        schema = vol.Schema(
            {
                vol.Required("confirm", default=False): selector.BooleanSelector()
            }
        )

        return self.async_show_form(
            step_id="confirm",
            data_schema=schema,
            description_placeholders=description_placeholders,
            errors=errors,
        )

    async def async_step_summary(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the summary step displaying the migration results."""
        if user_input is not None:
            init_data = self.context.get("init_data", {})
            mappings = self.context.get("mappings", [])

            # Save full migration context for options flow/recovery
            init_data["mappings"] = mappings
            init_data["cutoff_date"] = self.context.get("cutoff_date")
            init_data["delete_old"] = self.context.get("delete_old", False)
            init_data["influx_config"] = self.context.get("influx_config")
            init_data["influxdb_only"] = self.context.get("influxdb_only", False)

            title = "Device Migration"
            try:
                if len(mappings) == 1:
                    old_ent, new_ent = mappings[0]
                    old_clean = old_ent.split(".", 1)[1] if "." in old_ent else old_ent
                    new_clean = new_ent.split(".", 1)[1] if "." in new_ent else new_ent
                    title = f"{old_clean} -> {new_clean}"
                elif len(mappings) > 1:
                    mapped_strs = []
                    for old_ent, new_ent in mappings[:2]:
                        old_clean = old_ent.split(".", 1)[1] if "." in old_ent else old_ent
                        new_clean = new_ent.split(".", 1)[1] if "." in new_ent else new_ent
                        mapped_strs.append(f"{old_clean}->{new_clean}")
                    title = ", ".join(mapped_strs)
                    if len(mappings) > 2:
                        title += f" ... (+{len(mappings) - 2})"
                else:
                    old_entity = init_data.get(CONF_OLD_ENTITY_ID)
                    new_entity = init_data.get(CONF_NEW_ENTITY_ID)
                    if old_entity and new_entity:
                        title = f"{old_entity} -> {new_entity}"
            except Exception as e:
                _LOGGER.error("Error generating title: %s", e)

            return self.async_create_entry(
                title=f"Migration: {title}",
                data=init_data,
            )

        res = self.context.get("migration_result", {})
        details_text = "\n".join([f"- {d}" for d in res.get("details", [])])

        summary_text = (
            f"**Zusammenfassung der Migration:**\n\n"
            f"- **Status**: {res.get('status')}\n"
            f"- **Migrationstyp**: {res.get('migration_type')}\n"
            f"- **Daten gelöscht**: {res.get('deleted')}\n\n"
            f"**Details:**\n"
            f"{details_text}\n\n"
            f"Klicke auf 'Absenden' (Fertigstellen), um die Konfiguration abzuschließen."
        )
        summary_text = summary_text.replace("%", "%%")

        return self.async_show_form(
            step_id="summary",
            description_placeholders={"summary_text": summary_text},
            data_schema=vol.Schema({}),
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Create the options flow."""
        return EntityMigratorOptionsFlowHandler(config_entry)


class EntityMigratorOptionsFlowHandler(config_entries.OptionsFlow):
    """Handle options flow for Entity Statistics Migrator."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize options flow."""
        super().__init__()
        self._config_entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage the options."""
        errors = {}
        
        # Get existing mappings and config from entry data
        entry_data = self.config_entry.data
        mappings = entry_data.get("mappings", [])
        cutoff_date = entry_data.get("cutoff_date")
        delete_old = entry_data.get("delete_old", False)
        influx_config = entry_data.get("influx_config") or {}

        if user_input is not None:
            new_influx_config = {
                "host": user_input["influx_host"],
                "port": user_input["influx_port"],
                "database": user_input["influx_database"],
                "username": user_input.get("influx_username"),
                "password": user_input.get("influx_password"),
                "ssl": user_input.get("influx_ssl", False),
            }

            try:
                # Test connection
                from .influx_migrator import InfluxV1Migrator
                with InfluxV1Migrator(
                    host=new_influx_config["host"],
                    port=new_influx_config["port"],
                    database=new_influx_config["database"],
                    username=new_influx_config.get("username"),
                    password=new_influx_config.get("password"),
                    ssl=new_influx_config.get("ssl", False),
                ) as migrator:
                    await self.hass.async_add_executor_job(migrator.test_connection)

                # Run InfluxDB migration again (in the background)
                run_migration_in_background(
                    self.hass,
                    mappings,
                    cutoff_date,
                    delete_old,
                    new_influx_config,
                    True, # influxdb_only = True
                )
                
                # Store new influx config in context to save at the end of the flow
                self.context["new_influx_config"] = new_influx_config

                # Show results in a form
                self.context["summary_text"] = (
                    "**Migration im Hintergrund gestartet!**\n\n"
                    "Die Migration wurde asynchron im Hintergrund gestartet, um Timeouts zu verhindern.\n"
                    "Home Assistant benachrichtigt dich über das Glocken-Symbol unten links, sobald der Vorgang abgeschlossen ist."
                )
                return await self.async_step_summary_options()
            except Exception as e:
                _LOGGER.error("InfluxDB options migration failed: %s", e)
                errors["base"] = "influx_conn_error"

        schema = vol.Schema(
            {
                vol.Required("influx_host", default=influx_config.get("host", "localhost")): str,
                vol.Required("influx_port", default=influx_config.get("port", 8086)): int,
                vol.Required("influx_database", default=influx_config.get("database", "homeassistant")): str,
                vol.Optional("influx_username", default=influx_config.get("username", "")): str,
                vol.Optional("influx_password", default=influx_config.get("password", "")): str,
                vol.Optional("influx_ssl", default=influx_config.get("ssl", False)): selector.BooleanSelector(),
            }
        )

        return self.async_show_form(
            step_id="init", data_schema=schema, errors=errors
        )

    async def async_step_summary_options(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Finished summary page for options flow."""
        if user_input is not None:
            new_influx_config = self.context.get("new_influx_config")
            if new_influx_config:
                new_data = dict(self.config_entry.data)
                new_data["influx_config"] = new_influx_config
                self.hass.config_entries.async_update_entry(
                    self.config_entry, data=new_data
                )
            return self.async_create_entry(title="", data={})

        summary_text = self.context.get("summary_text", "")
        summary_text = summary_text.replace("%", "%%")
        return self.async_show_form(
            step_id="summary_options",
            description_placeholders={"summary_text": summary_text},
            data_schema=vol.Schema({}),
        )
