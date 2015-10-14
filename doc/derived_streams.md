## Creating a derived stream

- Follow the steps in the [`README`](../README.md) to set up a local pipeline.
- Create a Sandbox Filter to extract the information you want. A simple example is the [`payload_size.lua`](../heka/sandbox/filters/payload_size.lua) filter.
- Create a configuration file to test the filter during development. See [`payload_size_devel.toml`](../examples/payload_size_devel.toml) for an example config. For a derived stream based on Telemetry data, you will most likely use a `S3SplitFileInput` to read production data, and a `LogOutput` or `FileOutput` to view the resulting records locally.
- Create a [JSON filter](../examples/payload_size_devel_filter.json) to limit the input data to a reasonable amount for testing.
- Run it:
```bash
export PATH=$PATH:build/heka/build/heka/bin
hekad -config examples/payload_size_devel.toml
# You should see several "payload_size" messages logged to the console.
# Check the resulting file output:
heka-cat derived_data.out
```

