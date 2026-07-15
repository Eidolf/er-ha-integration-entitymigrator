# Entity Statistics Migrator for Home Assistant
<img src="https://raw.githubusercontent.com/Eidolf/er-ha-integration-entitymigrator/main/logo.png" alt="Logo" width="200">

This custom integration enables the migration of long-term statistics (LTS) and historical InfluxDB data points from an old entity to a new entity directly inside Home Assistant using a graphical user interface.

> [!WARNING]
> This integration directly modifies the Home Assistant database. Ensure you have a **working backup** before starting the migration process.

## Features

- **Direct Database Manipulation**: Works inside a database transaction via the SQLAlchemy session from the `recorder` component.
- **InfluxDB Support**: Optional migration of historical data points in InfluxDB (supports V1 API, connection tests, and automatic tag-key resolution).
- **Asynchronous Background Tasks**: Offloads database writes to a background worker thread. This keeps the UI responsive and prevents reverse proxy (e.g. Cloudflare HTTP 524) timeouts.
- **System Throttling**: Automatically throttles InfluxDB writes (250ms pause per 5,000 points and a 2-second cooldown pause every 25,000 points) to prevent Write-Ahead Log (WAL) locks and container crashes.
- **Estimated Duration**: Automatically counts SQL rows and InfluxDB points to show a time estimate before starting the migration.
- **Persistent Notifications**: Sends a Home Assistant notification (bell icon) with detailed statistics as soon as the background task finishes.
- **Multiple Migration Modes**:
  - **Geräte-Migration (Device Migration)**: Automatically maps all entities of a device to match the new device's entities.
  - **Einzelne Entität manuell migrieren (Single Entity)**: Migrate one specific sensor manually.
  - **Nur InfluxDB migrieren (InfluxDB Only)**: Re-run or run InfluxDB migrations without altering local SQL statistics.
- **Offset Calculation**: Calculates the difference in accumulated values for meters/sum sensors and adds the offset to prevent spikes in the Energy Dashboard.
- **Optional Cleanup**: Automatically purges remaining statistics and metadata of the old entity after migration.

## Installation

1. Copy `custom_components/entitymigrator/` to your Home Assistant's `custom_components/` directory (or install via HACS as a custom repository).
2. Restart Home Assistant.
3. In the Home Assistant UI, go to **Settings** -> **Devices & Services** -> **Add Integration**, and search for **Entity Statistics Migrator**.

## Config Flow / Usage

When starting a migration flow, you will configure:
1. **Mode**: Choose between Device Migration, Single Entity, or InfluxDB Only.
2. **Cutoff Date**: The transition timestamp. Statistics *before* this date will be migrated from the old entity to the new entity.
3. **Clean up**: A checkbox to decide whether the old entity's metadata should be purged.
4. **InfluxDB Config (Optional)**: Connection details for InfluxDB (Host, Port, Database, Credentials).
5. **Confirmation**: A screen summarizing warnings, found data points, and the **estimated migration duration**.
6. **Background Run**: Once started, the UI completes instantly. You can monitor progress in the Home Assistant logs and will receive a notification when finished.

## Troubleshooting & System Limits

### InfluxDB "Too Many Open Files" Crash
When migrating very large datasets spanning many years, InfluxDB must open WAL write files for many time-shards (weekly/monthly partitions) simultaneously. By default, the Home Assistant InfluxDB Add-on container limits open files to `1024`, causing the database to crash with a `too many open files` error and marking database segments as `.tsm.bad`.

#### 1. Permanent Automation Fix (Highly Recommended)
You can automate raising the file descriptor limit (ulimit) to `65536` on every system boot by using the **Advanced SSH & Web Terminal** Add-on:

1. Open the **Advanced SSH & Web Terminal** Add-on in your Home Assistant UI.
2. Under the **Info** tab, disable **Protection mode** (this allows the SSH container to run Docker commands).
3. Go to the **Configuration** tab, find the `init_commands` section, and paste the following snippet:
   ```yaml
   init_commands:
     - >-
       APP="addon_a0d7b954_influxdb";
       if docker ps --format '{{.Names}}' | grep -q "$APP"; then
         RUNFILE="/run/s6/legacy-services/influxdb/run";
         docker exec "$APP" sh -c "grep -q 'ulimit -n 65536' '$RUNFILE' || (cp '$RUNFILE' '$RUNFILE.bak' && sed -i '/exec influxd/i ulimit -n 65536' '$RUNFILE' && s6-svc -r /run/s6/legacy-services/influxdb)";
       fi
   ```
4. Click **Save** and restart the SSH Add-on. The patch will now run automatically on every system boot.

#### 2. How to Verify the Patch
To verify that the file descriptor limit of the running InfluxDB server has successfully been raised to `65536`, run this command in your SSH terminal:
```bash
docker exec addon_a0d7b954_influxdb sh -c 'cat /proc/$(pidof influxd)/limits | grep "open files"'
```
**Expected Output:**
```text
Max open files            65536                524288               files
```

#### 3. How to Restore `.tsm.bad` Files
If InfluxDB already crashed and renamed your data segments to `.bad` files, you can restore them:
1. Stop the InfluxDB service inside the container:
   ```bash
   docker exec addon_a0d7b954_influxdb s6-svc -d /run/s6/legacy-services/influxdb
   ```
2. Rename all `.bad` files back to `.tsm` in the database directory:
   ```bash
   docker exec addon_a0d7b954_influxdb sh -c "find /data/influxdb/data -name '*.tsm.bad' -exec sh -c 'for f do mv \"\$f\" \"\${f%.bad}\"; done' sh {} +"
   ```
3. Start the InfluxDB service again:
   ```bash
   docker exec addon_a0d7b954_influxdb s6-svc -u /run/s6/legacy-services/influxdb
   ```

- **Verbose Logs**: 
  The integration logs detailed progress and full error tracebacks. Check your `/config/home-assistant.log` file for `[SQL Migration]` and `[InfluxDB Validation]` warnings to inspect raw database queries and responses.
