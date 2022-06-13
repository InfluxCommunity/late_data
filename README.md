# Using Flux to Handle Late Arriving Data

## Overview

We will use Flux tasks and scripts to build a system that can recompute data for a given window when we detect that late data has arrived.

The example code use a fake data set that is measuring water levels at 100 different locations (i.e. tag `i` with values 0-99). We will be computing the mean water level at each location over 1 hour windows. We can handle data arriving up to 24 hours late.

We will use three buckets:

- water_level_raw: The raw data is written to this bucket
- water_level_mean_1h: The 1 hours means are stored here. This bucket is updated after recomputing a window
- water_level_checksum: Store a checksum (count) per 1 hour window for the last 24 hours

We will use a Flux task and a script:

- water_level_process (script): Computes the mean water level for a 1 hour period. This is a script so we can call it dynamically when we know a 1 hour window needs to be recomputed.
- water_level_checksum (task):  Computes the checksum of each 1h window, compares to existing checksums and calls the water_level_process script for each window that needs to be recomputed

These two Flux programs work together to ensure that late arriving data is correctly processed. The water_level_process script not only computes the mean for the period, but also write the new checksum to the `water_level_checksum` bucket. Thus helping to keep the checksum bucket in sync with the `water_level_mean_1h` bucket. 

The task periodically checks for differing checksums and triggers the script to recompute them. This allows the user to decide on what frequency they wish to check and recompute data windows independent of when the late data arrives.


## Writing Data

There is a python script `write.py` that writes data for each location every 10s. Additionally every 10s a late data point is written somewhere in the last 24 hours for each location.

This simulates both fresh data arrive at regular intervals and late data arriving spread out over the past.

## Notification

This branch also includes a Slack notification. This will trigger when late arriving data was detected. Here is the payload format:
```
WARNING: Late arriving data.
Details:
Measurment: water_level
Time: 2022-06-13T14:58:51.000000000Z
Old Count:  13809
New Count : 13889
Script trigger status:  200
```
*Note: Make sure you update the Slack hook url. This currently points to our community notification slack channel *

## Warning

While this process works, it is not fool proof there are a few failure modes to be aware of.

* If the `water_level_process` script fails for whatever reason it could write out the new checksum but fail to write the new mean thus creating an inconsistency.
* If the `water_level_process` script hits a race condition it could compute a mean that doesn't correspond with the checksum. 

Both of these failure modes exist within the `water_level_process` script and could potentially be address in the future by improving that script.

## Future work

Some features could be added to this system:

* Record a success/fail from the task for each window
* Add last updated to each window
