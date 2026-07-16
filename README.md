# Entity Statistics Migrator for Home Assistant
<img src="https://raw.githubusercontent.com/Eidolf/er-ha-integration-entitymigrator/main/logo.png" alt="Logo" width="200">

This custom integration enables the migration of long-term statistics (LTS) and historical InfluxDB data points from an old entity to a new entity directly inside Home Assistant using a graphical user interface. Additionally, it offers powerful tools to clean up unwanted data from InfluxDB.

> [!WARNING]
> This integration directly modifies the Home Assistant database. Ensure you have a **working backup** before starting the migration process.

## Features

- **Direct Database Manipulation**: Works inside a database transaction via the SQLAlchemy session from the `recorder` component.
- **InfluxDB Support**: Optional migration of historical data points in InfluxDB (supports V1 API, connection tests, and automatic tag-key resolution).
- **Asynchronous Background Tasks**: Offloads database writes and deletions to a background worker thread. This keeps the UI responsive and prevents reverse proxy (e.g. Cloudflare HTTP 524) timeouts.
- **System Throttling**: Automatically throttles InfluxDB writes (250ms pause per 5,000 points and a 2-second cooldown pause every 25,000 points) to prevent Write-Ahead Log (WAL) locks and container crashes.
- **Estimated Duration**: Automatically counts SQL rows and InfluxDB points to show a time estimate before starting the migration.
- **Persistent Notifications**: Sends a Home Assistant notification (bell icon) with detailed statistics as soon as a migration or cleanup finishes.
- **Multiple Migration & Cleanup Modes**:
  - **Geräte-Migration (Device Migration)**: Automatically maps all entities of a device to match the new device's entities.
  - **Einzelne Entität manuell migrieren (Single Entity)**: Migrate one specific sensor manually.
  - **Nur InfluxDB migrieren (InfluxDB Only)**: Re-run or run InfluxDB migrations without altering local SQL statistics.
  - **InfluxDB-Datenbank bereinigen (Cleanup)**: Scan and purge unwanted data from InfluxDB based on three configurable strategies:
    1. **Option 1 (yaml)**: Excluded entities/globs parsed from your `configuration.yaml` `influxdb:` exclusion rules.
    2. **Option 2 (migrated)**: Old source entities that have already been successfully migrated by this integration.
    3. **Option 3 (orphaned)**: Orphaned InfluxDB entities that no longer exist in Home Assistant's state list or Entity Registry.
- **Offset Calculation**: Calculates the difference in accumulated values for meters/sum sensors and adds the offset to prevent spikes in the Energy Dashboard.
- **Optional Cleanup**: Automatically purges remaining statistics and metadata of the old entity after migration.

## Installation

1. Copy `custom_components/entitymigrator/` to your Home Assistant's `custom_components/` directory (or install via HACS as a custom repository).
2. Restart Home Assistant.
3. In the Home Assistant UI, go to **Settings** -> **Devices & Services** -> **Add Integration**, and search for **Entity Statistics Migrator**.

## Config Flow / Usage

When starting a migration flow, you will configure:
1. **Mode**: Choose between Device Migration, Single Entity, Loop, or InfluxDB Cleanup.
2. **Cutoff Date / Options**: Configure time boundaries or select your InfluxDB cleanup strategy.
3. **InfluxDB Config (Optional)**: Connection details for InfluxDB (Host, Port, Database, Credentials).
4. **Confirmation**: A checklist of candidates (for cleanup) or a summary of warnings and duration (for migrations).
5. **Background Run**: The task executes in the background, and you receive a notification when finished.

## Optional: InfluxDB Tuning (ulimit / file descriptor limit)

When migrating very large historical datasets, InfluxDB must write to many time-shards simultaneously. To prevent InfluxDB from hitting the default container file descriptor limit (`too many open files`) and locking up, you can automatically increase the limit (ulimit) to `65536` on startup.

### 1. Automation via SSH Terminal Add-on
You can automate this via the **Advanced SSH & Web Terminal** Add-on:

1. Open the **Advanced SSH & Web Terminal** Add-on.
2. Under the **Info** tab, disable **Protection mode** (enables Docker commands).
3. Go to the **Configuration** tab, find `init_commands`, and paste this background-loop script:
   ```yaml
   init_commands:
     - >-
       (
       APP="addon_a0d7b954_influxdb";
       for i in $(seq 1 30); do
         if docker ps --format "{{.Names}}" | grep -q "$APP"; then
           RUNFILE="/run/s6/legacy-services/influxdb/run";
           docker exec "$APP" sh -c "grep -q 'ulimit -n 65536' '$RUNFILE' || (cp '$RUNFILE' '$RUNFILE.bak' && sed -i '/exec influxd/i ulimit -n 65536' '$RUNFILE' && s6-svc -r /run/s6/legacy-services/influxdb)";
           break;
         fi;
         sleep 2;
       done
       ) &
   ```
4. Click **Save** and restart the SSH Add-on.

> [!NOTE]
> Once you have successfully completed all your historical InfluxDB migrations, you can safely delete this script from your SSH Add-on's `init_commands` to restore your InfluxDB container to its default system limits.

### 2. Verify the Active Limits
To check if the ulimit of the running InfluxDB server has successfully been raised to `65536`, run this command in your SSH terminal:
```bash
docker exec addon_a0d7b954_influxdb sh -c 'cat /proc/$(pidof influxd)/limits | grep "open files"'
```
**Expected Output:**
```text
Max open files            65536                524288               files
```

## Troubleshooting

- **Verbose Logs**: 
  The integration logs detailed progress and full error tracebacks. Check your `/config/home-assistant.log` file for `[SQL Migration]` and `[InfluxDB Validation]` warnings to inspect raw database queries and responses.
