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

    session = get_instance(hass).get_session()
    try:
        # 1. Determine active time column (start_ts or start)
        test_query = session.execute(text("SELECT * FROM statistics LIMIT 1"))
        has_start_ts = "start_ts" in test_query.keys()
        time_col = "start_ts" if has_start_ts else "start"
        time_val = ts if has_start_ts else dt

        has_lts_any = False
        has_states_any = False

        for old_entity, new_entity in mappings:
            if not old_entity or not new_entity:
                continue

            # 2. Cleanup old states history if requested
            if delete_old:
                # Check if states table has metadata_id (modern HA versions) or entity_id (older HA versions)
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
                
                session.commit()
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
                # Get unit of measurement from states if new metadata doesn't exist yet
                new_state = hass.states.get(new_entity)
                if new_state:
                    new_unit = new_state.attributes.get("unit_of_measurement")

            # Validate unit matching
            if old_unit and new_unit:
                old_u_norm = old_unit.strip().lower()
                new_u_norm = new_unit.strip().lower()
                if old_u_norm != new_u_norm:
                    raise ValueError(f"UNIT_MISMATCH:{old_entity}:{new_entity}:{old_unit}:{new_unit}")

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
                        "unit": old_unit,
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

            # 5. Unique-Constraint-Bereinigung (Delete stats of the new entity before cutoff_date)
            session.execute(
                text(f"DELETE FROM statistics WHERE metadata_id = :new_id AND {time_col} < :time_val"),
                {"new_id": new_meta_id, "time_val": time_val}
            )
            session.execute(
                text(f"DELETE FROM statistics_short_term WHERE metadata_id = :new_id AND {time_col} < :time_val"),
                {"new_id": new_meta_id, "time_val": time_val}
            )
            session.commit()

            # 6. Offset-Berechnung (for counters/sums)
            if has_sum:
                # Last sum of old entity before cutoff_date
                old_sum_row = session.execute(
                    text(f"SELECT sum FROM statistics WHERE metadata_id = :old_id AND {time_col} < :time_val ORDER BY {time_col} DESC LIMIT 1"),
                    {"old_id": old_meta_id, "time_val": time_val}
                ).fetchone()

                # Fallback 1: If no old sum before cutoff_date, get the absolute first record of the old entity
                if old_sum_row is None or old_sum_row[0] is None:
                    old_sum_row = session.execute(
                        text(f"SELECT sum FROM statistics WHERE metadata_id = :old_id ORDER BY {time_col} ASC LIMIT 1"),
                        {"old_id": old_meta_id}
                    ).fetchone()

                # First sum of new entity after cutoff_date
                new_sum_row = session.execute(
                    text(f"SELECT sum FROM statistics WHERE metadata_id = :new_id AND {time_col} >= :time_val ORDER BY {time_col} ASC LIMIT 1"),
                    {"new_id": new_meta_id, "time_val": time_val}
                ).fetchone()

                # Fallback 2: If no new sum after cutoff_date, get the absolute first record of the new entity
                if new_sum_row is None or new_sum_row[0] is None:
                    new_sum_row = session.execute(
                        text(f"SELECT sum FROM statistics WHERE metadata_id = :new_id ORDER BY {time_col} ASC LIMIT 1"),
                        {"new_id": new_meta_id}
                    ).fetchone()

                # Fallback 3: If still no new sum, use the current state value of the new entity in HA
                if new_sum_row is None or new_sum_row[0] is None:
                    new_state = hass.states.get(new_entity)
                    if new_state is not None:
                        try:
                            val = float(new_state.state)
                            new_sum_row = (val,)
                        except (ValueError, TypeError):
                            pass

                if old_sum_row is not None and old_sum_row[0] is not None and new_sum_row is not None and new_sum_row[0] is not None:
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
                        session.commit()

            # 7. Perform the migration (UPDATE)
            session.execute(
                text(f"UPDATE statistics SET metadata_id = :new_id WHERE metadata_id = :old_id AND {time_col} < :time_val"),
                {"new_id": new_meta_id, "old_id": old_meta_id, "time_val": time_val}
            )
            session.execute(
                text(f"UPDATE statistics_short_term SET metadata_id = :new_id WHERE metadata_id = :old_id AND {time_col} < :time_val"),
                {"new_id": new_meta_id, "old_id": old_meta_id, "time_val": time_val}
            )
            session.commit()

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
                session.commit()

            result_summary["details"].append(f"{old_entity} -> {new_entity}: Erfolgreich")

        if not has_lts_any:
            result_summary["status"] = "Erfolgreich (keine LTS vorhanden)"
            result_summary["migration_type"] = "Nur Kurzzeit-Zustände (Zustandsverlauf)"
        elif has_states_any:
            result_summary["migration_type"] = "LTS & Kurzzeit-Zustände"

        return result_summary
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
        """Handle the initial setup mode step."""
        errors = {}

        if user_input is not None:
            # Save configuration settings globally in flow context
            self.context["mode"] = user_input[CONF_MODE]
            self.context["cutoff_date"] = user_input[CONF_CUTOFF_DATE]
            self.context["delete_old"] = user_input.get(CONF_DELETE_OLD, False)
            self.context["mappings"] = []

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
            }
        )

        return self.async_show_form(
            step_id="user", data_schema=schema, errors=errors
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

        options_list = []
        for entity_id in sorted(list(all_entities)):
            state = self.hass.states.get(entity_id)
            val_info = ""
            if state:
                val = state.state
                unit = state.attributes.get("unit_of_measurement", "")
                val_info = f" (Aktuell: {val} {unit})"
            options_list.append(
                selector.SelectOptionDict(
                    value=entity_id,
                    label=f"{entity_id}{val_info}"
                )
            )

        if user_input is not None:
            mappings = []
            for entry in device_entities:
                key_base = entry.entity_id.replace(".", "__")
                matched_key = None
                for k in user_input:
                    if k.startswith(key_base):
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
                    summary = await get_instance(self.hass).async_add_executor_job(
                        run_db_migration,
                        self.hass,
                        mappings,
                        self.context["cutoff_date"],
                        self.context["delete_old"],
                    )
                    device_registry = dr.async_get(self.hass)
                    device_entry = device_registry.async_get(device_id)
                    device_name = device_id
                    if device_entry:
                        device_name = device_entry.name_by_user or device_entry.name or device_id

                    self.context["init_data"] = {
                        CONF_OLD_ENTITY_ID: f"Device Migration ({len(mappings)} Entitäten)",
                        CONF_NEW_ENTITY_ID: device_name,
                    }
                    self.context["migration_result"] = summary
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
                                    val_info = f" (Aktuell: {val} {unit})"
                                found_key = f"{bad_key_base}{val_info}"
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
                val_info = f" (Aktuell: {val} {unit})"
            
            key = f"{key_base}{val_info}"
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

        options_list = []
        for entity_id in sorted(list(all_entities)):
            state = self.hass.states.get(entity_id)
            val_info = ""
            if state:
                val = state.state
                unit = state.attributes.get("unit_of_measurement", "")
                val_info = f" (Aktuell: {val} {unit})"
            options_list.append(
                selector.SelectOptionDict(
                    value=entity_id,
                    label=f"{entity_id}{val_info}"
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
                    summary = await get_instance(self.hass).async_add_executor_job(
                        run_db_migration,
                        self.hass,
                        self.context["mappings"],
                        self.context["cutoff_date"],
                        self.context["delete_old"],
                    )
                    self.context["init_data"] = {
                        CONF_OLD_ENTITY_ID: f"Migration ({len(self.context['mappings'])} Entitäten)",
                        CONF_NEW_ENTITY_ID: f"{len(self.context['mappings'])} Ziele",
                    }
                    self.context["migration_result"] = summary
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

    async def async_step_summary(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the summary step displaying the migration results."""
        if user_input is not None:
            init_data = self.context.get("init_data", {})
            old_entity = init_data.get(CONF_OLD_ENTITY_ID)
            new_entity = init_data.get(CONF_NEW_ENTITY_ID)
            return self.async_create_entry(
                title=f"Migration: {old_entity} -> {new_entity}",
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

        return self.async_show_form(
            step_id="summary",
            description_placeholders={"summary_text": summary_text},
            data_schema=vol.Schema({}),
        )
