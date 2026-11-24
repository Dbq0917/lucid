<p align="center">
  <p align="center">
    <img src="https://github.com/lucid-kv/deploy-templates/blob/master/lucid.png?raw=true" height="100" alt="Lucid KV" />
  </p>
  <h3 align="center">
    About Lucid KV
  </h3>
  <p align="center">
    High performance and distributed KV store w/ REST API. 🦀
  </p>
  <p align="center">
    <a href="https://github.com/lucid-kv/lucid/actions?workflow=Lucid"><img src="https://github.com/lucid-kv/lucid/workflows/Lucid/badge.svg" /></a>
    <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/Made%20With-Rust-dea584" /></a>
    <a href="https://github.com/lucid-kv/lucid/blob/master/LICENSE.md"><img src="https://img.shields.io/badge/license-MIT-lightgrey.svg" /></a>
    <a href="https://discord.gg/mZz67M6"><img src="https://img.shields.io/badge/Discord-Server-7289DA" /></a>
  </p>
</p>

## Introduction

Lucid is a high performance, secure and distributed key-value store accessible through an HTTP API, that is built around a modular configuration to enable features on the fly, like persistence, encryption SSE, compression, replication, and more.

[Read the complete Medium article →](https://medium.com/@clint21/lucid-an-http-key-value-store-c0e734586e26)

## Getting Started

Get the latest binary from the [releases](https://github.com/lucid-kv/lucid/releases) page and run these commands:

```
$ ./lucid init
$ ./lucid server
```

### Docker Deployment

Or run a node with Docker, but you need to create a [lucid.yml](.github/lucid.yml) file locally before.

```
$ docker pull lucidkv/lucid
$ docker run -p 7020:7020 -v lucid.yml:/etc/lucid/lucid.yml lucidkv/lucid
```

A demonstration node is accessible at <https://lucid-kv.herokuapp.com/> or deploy your own:

<a href="https://heroku.com/deploy?template=https://github.com/lucid-kv/lucid" target="_blank"><img src="https://www.herokucdn.com/deploy/button.svg" height="32"/></a>
<a href="https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Flucid-kv%2Flucid%2Fmaster%2F.github%2Fazure%2Fazuredeploy.json" target="_blank"><img src="https://aka.ms/deploytoazurebutton" height="32"/></a>

## Documentation

You can quickly start [here](https://github.com/lucid-kv/lucid/wiki) or get the complete documentation at <https://clintnetwork.gitbook.io/lucid/>.

## Persistence

Persistence is enabled by default. Lucid appends every mutation to a journal and syncs it in the background, so writes are not blocked by an `fsync` on every request. The journal is compacted into an atomic snapshot after enough records have accumulated, and pending changes are synced when the store is closed normally.

Set `persistence.location` to a file or directory to choose the journal location. When it is empty, Lucid uses the platform application data directory. Set `persistence.enabled` to `false` to run with an in-memory store.

## Command Line Interface

You can take a look at the Lucid command line at <https://asciinema.org/a/277538>.

```
 ██╗    ██╗   ██╗ ██████╗██╗██████╗     ██╗  ██╗██╗   ██╗
 ██║    ██║   ██║██╔════╝██║██╔══██╗    ██║ ██╔╝██║   ██║
 ██║    ██║   ██║██║     ██║██║  ██║    ██╔═██╗ ╚██╗ ██╔╝
 ██████╗╚██████╔╝╚██████╗██║██████╔╝    ██║  ██╗ ╚████╔╝
 ╚═════╝ ╚═════╝  ╚═════╝╚═╝╚═════╝     ╚═╝  ╚═╝  ╚═══╝

A Fast, Secure and Distributed KV store with an HTTP API.
Written in Rust, Fork us on GitHub (https://github.com/lucid-kv)

FLAGS:
    -h, --help         Prints help information
        --no-banner    Disable showing the banner on start
    -V, --version      Prints version information

OPTIONS:
    -c, --config <config>    Specify the Lucid configuration file

SUBCOMMANDS:
    help        Prints this message or the help of the given subcommand(s)
    init        Initialize Lucid and generate configuration file
    server      Run a new Lucid server instance
    settings    Manage the Lucid configuration file
```

## Web Interface (UI)

Lucid wants to propose a Web UI to manage data, issue tokens, organize nodes and configure instances.

## Development Credits

Lucid is developed by [@thisisclint21](https://github.com/thisisclint21), [@Slals](https://github.com/Slals), [@CephalonRho](https://github.com/CephalonRho), [@rigwild](https://github.com/rigwild) and published under the [MIT License](LICENSE.md).

| Name / Nickname | Twitter               | Role               |
|-----------------|-----------------------|--------------------|
| clint21.eth ⚡️   | @thisisclint21        | Lucid Founder      |
| CephalonRho     | @cephalonrho          | Core Development   |
| Jonathan Serra  | @_blocs               | Core Development   |
| Rigwild         | @rigwild_             | Web UI Development |

## Contribute to Lucid

See [CONTRIBUTING.md](CONTRIBUTING.md) for best practices and instructions on setting up your development environment to work on Lucid.
