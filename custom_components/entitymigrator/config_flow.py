"""Config flow for Entity Statistics Migrator integration."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import voluptuous as vol
from sqlalchemy import text

from homeassistant import config_entries
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import list_statistic_ids
from homeassistant.core import HomeAssistant, callback
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import selector
import homeassistant.util.dt as dt_util

from .const import (
    CONF_CUTOFF_DATE,
    CONF_DELETE_OLD,
    CONF_NEW_ENTITY_ID,
    CONF_OLD_ENTITY_ID,
    DOMAIN,
)
_LOGGER = logging.getLogger(__name__)

class NoStatistics(Exception):
    """Error to indicate that the old entity does not have statistics."""



def run_db_migration(
    hass: HomeAssistant,
    old_entity: str,
    new_entity: str,
    cutoff_date_str: str,
    delete_old: bool,
) -> None:
    """Run the database migration transaction in the executor thread."""
    dt = dt_util.parse_datetime(cutoff_date_str)
    if dt is None:
        raise ValueError("Invalid datetime format")
    dt = dt_util.as_utc(dt)
    ts = dt.timestamp()

    session = get_instance(hass).get_session()
    try:
        # 1. Determine active time column (start_ts or start)
        test_query = session.execute(text("SELECT * FROM statistics LIMIT 1"))
        has_start_ts = "start_ts" in test_query.keys()
        time_col = "start_ts" if has_start_ts else "start"
        time_val = ts if has_start_ts else dt

        # 2. Get old metadata
        old_meta = session.execute(
            text("SELECT id, has_sum, source, unit_of_measurement, has_mean FROM statistics_meta WHERE statistic_id = :old"),
            {"old": old_entity}
        ).fetchone()

        if not old_meta:
            raise NoStatistics(f"Old entity {old_entity} has no statistics metadata")

        old_meta_id, has_sum, source, unit, has_mean = old_meta

        # 3. Get or create new metadata
        new_meta = session.execute(
            text("SELECT id FROM statistics_meta WHERE statistic_id = :new"),
            {"new": new_entity}
        ).fetchone()

        if not new_meta:
            # Create metadata entry for new entity
            session.execute(
                text(
                    "INSERT INTO statistics_meta (statistic_id, source, unit_of_measurement, has_mean, has_sum) "
                    "VALUES (:new, :source, :unit, :has_mean, :has_sum)"
                ),
                {
                    "new": new_entity,
                    "source": source or "recorder",
                    "unit": unit,
                    "has_mean": has_mean,
                    "has_sum": has_sum,
                }
            )
            session.commit()
            new_meta = session.execute(
                text("SELECT id FROM statistics_meta WHERE statistic_id = :new"),
                {"new": new_entity}
            ).fetchone()

        new_meta_id = new_meta[0]

        # 4. Unique-Constraint-Bereinigung (Delete stats of the new entity before cutoff_date)
        session.execute(
            text(f"DELETE FROM statistics WHERE metadata_id = :new_id AND {time_col} < :time_val"),
            {"new_id": new_meta_id, "time_val": time_val}
        )
        session.execute(
            text(f"DELETE FROM statistics_short_term WHERE metadata_id = :new_id AND {time_col} < :time_val"),
            {"new_id": new_meta_id, "time_val": time_val}
        )

        # 5. Offset-Berechnung (for counters/sums)
        if has_sum:
            # Last sum of old entity before cutoff_date
            old_sum_row = session.execute(
                text(f"SELECT sum FROM statistics WHERE metadata_id = :old_id AND {time_col} < :time_val ORDER BY {time_col} DESC LIMIT 1"),
                {"old_id": old_meta_id, "time_val": time_val}
            ).fetchone()

            # First sum of new entity after cutoff_date
            new_sum_row = session.execute(
                text(f"SELECT sum FROM statistics WHERE metadata_id = :new_id AND {time_col} >= :time_val ORDER BY {time_col} ASC LIMIT 1"),
                {"new_id": new_meta_id, "time_val": time_val}
            ).fetchone()

            if old_sum_row is not None and new_sum_row is not None:
                old_sum = old_sum_row[0] or 0.0
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

        # 6. Perform the migration (UPDATE)
        session.execute(
            text(f"UPDATE statistics SET metadata_id = :new_id WHERE metadata_id = :old_id AND {time_col} < :time_val"),
            {"new_id": new_meta_id, "old_id": old_meta_id, "time_val": time_val}
        )
        session.execute(
            text(f"UPDATE statistics_short_term SET metadata_id = :new_id WHERE metadata_id = :old_id AND {time_col} < :time_val"),
            {"new_id": new_meta_id, "old_id": old_meta_id, "time_val": time_val}
        )

        # 7. Cleanup old entity if requested
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

        session.commit()
    except Exception as err:
        session.rollback()
        _LOGGER.error("Database migration error: %s", err)
        raise err
    finally:
        session.close()

class EntityMigratorConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Entity Statistics Migrator."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step."""
        errors = {}

        # Retrieve entities from statistics_meta using Home Assistant recorder API
        try:
            stats = await get_instance(self.hass).async_add_executor_job(
                list_statistic_ids, self.hass
            )
            old_entities_map = {
                item["statistic_id"]: bool(item.get("has_sum"))
                for item in stats
                if "statistic_id" in item
            }
        except Exception as err:
            _LOGGER.error("Failed to list statistic IDs: %s", err)
            old_entities_map = {}

        if user_input is not None:
            old_entity = user_input[CONF_OLD_ENTITY_ID]
            new_entity = user_input[CONF_NEW_ENTITY_ID]
            cutoff_date = user_input[CONF_CUTOFF_DATE]
            delete_old = user_input.get(CONF_DELETE_OLD, False)

            if old_entity == new_entity:
                errors["base"] = "same_entity"
            else:
                try:
                    await get_instance(self.hass).async_add_executor_job(
                        run_db_migration,
                        self.hass,
                        old_entity,
                        new_entity,
                        cutoff_date,
                        delete_old,
                    )
                    return self.async_create_entry(
                        title=f"Migration: {old_entity} -> {new_entity}",
                        data=user_input,
                    )
                except NoStatistics:
                    errors["base"] = "no_statistics"
                except Exception:
                    errors["base"] = "db_error"

        # Combine active state entities with stats entities for selection lists
        all_entities = set(self.hass.states.async_entity_ids())
        all_entities.update(old_entities_map.keys())

        old_options = sorted(list(all_entities))
        if not old_options:
            old_options = ["Keine Entitäten mit Statistiken gefunden"]

        new_options = sorted(list(all_entities))
        if not new_options:
            new_options = ["Keine Entitäten gefunden"]

        schema = vol.Schema(
            {
                vol.Required(CONF_OLD_ENTITY_ID): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=old_options,
                        mode=selector.SelectSelectorMode.DROPDOWN,
                        custom_value=True,
                    )
                ),
                vol.Required(CONF_NEW_ENTITY_ID): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=new_options,
                        mode=selector.SelectSelectorMode.DROPDOWN,
                        custom_value=True,
                    )
                ),
                vol.Required(CONF_CUTOFF_DATE): selector.DateTimeSelector(),
                vol.Optional(CONF_DELETE_OLD, default=False): selector.BooleanSelector(),
            }
        )

        return self.async_show_form(
            step_id="user", data_schema=schema, errors=errors
        )
