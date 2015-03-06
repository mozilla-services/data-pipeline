# Mozilla Services Data Pipeline

This repository contains the extra bits and pieces needed to build heka
for use in the [Cloud Services Data Pipeline](https://wiki.mozilla.org/CloudServices/DataPipeline).

Visit us on irc.mozilla.org in `#datapipeline`.

## Building a Data Pipeline RPM

Run `bin/build_pipeline_heka.sh` from the top level of this repo to build a heka RPM.

## Using the Data Pipeline

If you are simply looking to test out some data analysis plugins and don't want to setup your own pipeline here is the fastest way to get going:
https://mana.mozilla.org/wiki/display/CLOUDSERVICES/Using+the+sandbox+manager+in+the+dev+prototype+pipeline

## Running/Testing Your Own Data Pipeline

You can set up a bare-bones data pipeline of your own.  You will get an endpoint that listens for HTTP POST requests, performs GeoIP lookups, and wraps them up in protobuf messages. These messages will be relayed to a stream-processor, and will be output to a local store on disk. There will be basic web-based monitoring, and the ability to add your own stream processing filters.

1. Clone this data-pipeline github repo

    ```
    git clone https://github.com/mozilla-services/data-pipeline.git
    ```

2. Build and configure heka. If you are unable to build heka, drop by #datapipeline on irc.mozilla.org and we will try to provide you a pre-built version.
  1. Run `bash bin/build_pipeline_heka.sh`
  2. Install lua modules

        ```
        mkdir lua_modules
        rsync -av build/heka/build/heka/modules/ lua_modules/
        ```

  3. Procure a `GeoLiteCity.dat` file and put it in the current dir

        ```
        wget http://geolite.maxmind.com/download/geoip/database/GeoLiteCity.dat.gz
        ```

3. Set up the main Pipeline using the `examples/basic_local_pipeline.toml` config file. This will listen for HTTP POSTs on port 8080, log the raw and decoded messages requests to stdout, run the example filter, and output the records to a file.

    ```
    build/heka/build/heka/bin/hekad -config examples/basic_local_pipeline.toml
    ```

4. Check the monitoring dashboard at [http://localhost:4352](http://localhost:4352)
5. Fire off some test submissions!

    ```
    for f in $(seq 1 20); do
      curl -X POST "http://localhost:8080/submit/test/$f/foo/bar/baz" -d "{\"test\":$f}"
    done
    ```

6. Verify that your data was stored in the output file using the `heka-cat` utility

    ```
    build/heka/build/heka/bin/heka-cat data_raw.out
    build/heka/build/heka/bin/heka-cat data_decoded.out
    ```

7. Experiment with sandbox filters, outputs, and configurations.

### Useful things to know

- GeoIP
  - It’s not terribly interesting to do GeoIP lookups on 127.0.0.1, so you may want to provide a `--header "X-Forwarded-For: 8.8.8.8"` argument to your curl commands. That will force a geoIP lookup on the specified IP address (Google’s DNS server in this example).
- How to configure namespaces
  - The example config allows submissions to either `/submit/telemetry/docid/more/path/stuff` or `/submit/test/id/and/so/on`
  - You can add more endpoints by modifying the `namespace_config` parameter in `basic_local_pipeline.edge.toml`.
  - The namespace config is more manageable if you the JSON in a separate file, and run it through something like `jq -c '.' < my_namespaces.json` before putting it into the toml config.
- Where to get more info about configuring heka
  - http://hekad.readthedocs.org/en/latest/index.html
