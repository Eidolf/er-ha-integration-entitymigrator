# Entity Statistics Migrator for Home Assistant
<img src="https://raw.githubusercontent.com/Eidolf/er-ha-integration-entitymigrator/main/logo.png" alt="Logo" width="200">

This custom integration enables the migration of long-term statistics (LTS) from an old entity to a new entity directly inside Home Assistant using a graphical user interface (Config Flow).

> [!WARNING]
> This integration directly modifies the Home Assistant database. Ensure you have a **working backup** before starting the migration process.

## Features

- **Direct Database Manipulation**: Works inside a database transaction via the SQL Alchemy session from the `recorder` component.
- **Unique Constraint Resolution**: Prevents migration conflicts by cleaning up colliding records for the target entity.
- **Offset Calculation**: Calculates the difference in accumulated values for meters/sum sensors and adds the offset to prevent spikes in your Energy Dashboard.
- **Optional Cleanup**: Allows you to automatically purge remaining statistics and metadata of the old entity after migration.

## Installation

1. Copy `custom_components/entitymigrator/` to your Home Assistant's `custom_components/` directory (or install via HACS as a custom repository).
2. Restart Home Assistant.
3. In the Home Assistant UI, go to **Settings** -> **Devices & Services** -> **Add Integration**, and search for **Entity Statistics Migrator**.

## Config Flow / Usage

When setting up the integration, you will be prompted to fill out:
1. **Alte Entität (Quelle)**: The old entity whose historical data you want to migrate.
2. **Neue Entität (Ziel)**: The new entity that should receive the statistics.
3. **Schnittdatum & Uhrzeit**: The transition timestamp. Statistics *before* this date/time will be migrated from the old entity to the new entity.
4. **Clean up**: A checkbox to decide whether the old entity and its metadata should be completely deleted from the database.

## Technical Details

The integration executes the following steps inside a single database transaction:
1. Deletes records from the target entity's `statistics` and `statistics_short_term` tables where `start_ts` is before the cutoff date.
2. Computes the value difference (offset) between the old entity's last sum and the new entity's first sum at the boundary.
3. If the sensors are counters (having `has_sum` enabled in metadata), adjusts the subsequent `sum` values on the target entity to avoid sudden spikes.
4. Updates all source entity's statistics records before the cutoff date to target the new metadata ID.
5. If requested, deletes all remaining statistics and metadata of the old entity.
