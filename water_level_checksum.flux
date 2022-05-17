import "influxdata/influxdb/secrets"
import "experimental/http/requests"
import "json"
import "date"
import "experimental"

option task = {name: "water_level_checksum", every: 5m, offset: 1m}

token = secrets.get(key: "SELF_TOKEN")

// invokeScript calls a Flux script with the given start stop
// parameters to recompute the window.
invokeScript = (start, stop) => {
    response =
        requests.post(
            // We have hardcoded the script ID here
            url: "https://eastus-1.azure.cloud2.influxdata.com/api/v2/scripts/095fabd404108000/invoke",
            headers:
                ["Authorization": "Token ${token}", "Accept": "application/json", "Content-Type": "application/json"],
            body: json.encode(v: {params: {start: string(v: start), stop: string(v: stop)}}),
        )

    return response.statusCode
}

// Only query windows that span a full hour
start = date.truncate(t: -1d, unit: 1h)
stop = date.truncate(t: now(), unit: 1h)

newCounts =
    from(bucket: "water_level_raw")
        |> range(start: start, stop: stop)
        |> group(columns: ["_measurement", "_field"])
        |> aggregateWindow(every: 1h, fn: count)

oldCounts =
    from(bucket: "water_level_checksum")
        |> range(start: start, stop: stop)
        |> group(columns: ["_measurement", "_field"])

// Compare old and new checksum
experimental.join(
    left: oldCounts,
    right: newCounts,
    fn: (left, right) => ({left with old_count: left._value, new_count: right._value}),
)
    // Recompute any windows where the checksum is different
    |> filter(fn: (r) => r.old_count != r.new_count)
    |> map(
        fn: (r) => {
            status = invokeScript(start: date.sub(d: 1h, from: r._time), stop: r._time)

            return {r with status: status}
        },
    )
    |> yield(name: "diffs")