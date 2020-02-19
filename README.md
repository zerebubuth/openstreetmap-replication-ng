# openstreetmap-replication-ng

Script for doing OSM (minutely) replication using [osm-logical](https://github.com/zerebubuth/osm-logical) and [joto's osmdbt](https://github.com/joto/osmdbt).

## Installation

First, install and set up [osm-logical](https://github.com/zerebubuth/osm-logical). Below is a summary, for detailed information please look at the project's [`README`](https://github.com/zerebubuth/osm-logical/blob/master/README.md).

```
sudo apt install build-essential postgresql postgresql-server-dev-all
git clone https://github.com/zerebubuth/osm-logical.git
cd osm-logical
make && sudo make install
```

Second, install [OSMDBT](https://github.com/joto/osmdbt). Again, summarised below, see project [README](https://github.com/joto/osmdbt/blob/master/README.md) for more information.

```
sudo apt install libosmium2-dev libprotozero-dev libboost-program-options-dev libbz2-dev zlib1g-dev libexpat1-dev cmake libyaml-cpp-dev libpqxx-dev pandoc
git clone --branch develop https://github.com/zerebubuth/osmdbt.git
cd osmdbt
mkdir build && cd build
cmake ..
make
```

Third, clone this repository:

```
sudo apt install ruby
git clone https://github.com/zerebubuth/openstreetmap-replication-ng.git
cd openstreetmap-replication-ng
```

Configure OSMDBT and this repository. There are example configs in both directories. Basically, you need to tell OSMDBT which database to connect to. You can run `osmdbt-testdb` to test your settings.

Create a replication slot:

```
../osmdbt/build/src/osmdbt-enable-replication --config /path/to/your/osmdbt_config.yaml
```

Create an initial `state.txt` in the `base-dir` you configured in `config.yaml`. A simple one to start with might be something like:

```
sequenceNumber=1
timestamp=1970-01-01T00:00:00Z
lsn=0/00000000
```

Note that, although the LSN is at zero, the replication will start from when the replication slot was created. It doesn't seem to be possible to set the LSN at which replication should start.

You can run:

```
ruby replicate.rb --config config.yaml
```

And this should do a single replication "step". In other words, if you run it every minute then it should output minutely diffs. Note that it does _not_ output empty diffs if there were no changes.
