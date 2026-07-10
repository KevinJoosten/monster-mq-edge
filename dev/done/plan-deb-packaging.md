# Plan: Cross-Compiling and Packaging MonsterMQ Edge as a .deb Package on macOS

This plan outlines how to compile the MonsterMQ Edge broker for ARM-based Raspberry Pi devices (both 32-bit and 64-bit) on a macOS host, and package it as a `.deb` Debian package using standard command-line tools without requiring `dpkg-deb` or a Linux environment.

## 1. Feasibility & Method

Since MonsterMQ Edge is written in pure Go without CGO dependencies, cross-compilation is fully supported out-of-the-box by Go's toolchain on macOS.
A `.deb` package file is a standard Unix `ar` archive containing three files in a specific order:
1. `debian-binary`: a text file containing `2.0\n` specifying the package format version.
2. `control.tar.gz`: a gzipped tarball containing the package metadata (`control` file) and install scripts (`postinst`, `prerm`, `postrm`).
3. `data.tar.gz`: a gzipped tarball containing the files to be installed on the target system (binary, systemd service, config files).

By using standard Go cross-compilation, macOS `tar` (with env var `COPYFILE_DISABLE=1` to prevent macOS `._` metadata files), and macOS `ar`, we can build fully compliant Debian packages directly on a Mac.

---

## 2. Directory Layout & Package Structure

During the build process, we will create a temporary staging directory `dist/` with the following structure:

```text
dist/
├── debian-binary
├── control/
│   ├── control
│   ├── postinst
│   ├── prerm
│   └── postrm
├── data/
│   ├── usr/
│   │   └── local/
│   │       └── bin/
│   │           └── monstermq-edge
│   ├── etc/
│   │   └── monstermq/
│   │       └── config.yaml  (copied from scripts/deb/config.yaml)
│   └── lib/
│       └── systemd/
│           └── system/
│               └── monstermq-edge.service (copied from systemd/monstermq-edge.service)
```

---

## 3. Package Specification Files

### 3.1 `control` File
The metadata file defining the package details:
```text
Package: monstermq-edge
Version: <VERSION>
Section: misc
Priority: optional
Architecture: <DEB_ARCH> (arm64 or armhf)
Maintainer: info@monstermq.com
Description: MonsterMQ Edge MQTT Broker
 A single-binary, single-node MQTT broker for edge devices.
```

### 3.2 `postinst` (Post-installation Script)
Handles:
- Creating the `monstermq` system group and user if they do not exist.
- Setting correct permissions on the configuration directory `/etc/monstermq` and data directories.
- Reloading the systemd daemon.
- Enabling and starting/restarting the `monstermq-edge` service.

### 3.3 `prerm` (Pre-removal Script)
Handles:
- Stopping and disabling the systemd service before package removal.

### 3.4 `postrm` (Post-removal Script)
Handles:
- Removing user and configuration files on package purge (`apt purge`).
- Reloading the systemd daemon.

---

## 4. Implementation Steps

### Phase 1: Create Build Scripts and Metadata Templates
1. Write the package maintainer scripts (`postinst`, `prerm`, `postrm`) and template files.
2. Store scripts and metadata templates under a new directory `scripts/deb/` to keep the root directory clean.
3. Write `scripts/build-deb.sh` to coordinate the compilation and packaging process.

### Phase 2: Compilation & Staging
The script will:
1. Parse arguments (e.g. `--arch arm64` or `--arch armhf`).
2. Read the version from `version.txt`. If version details include git SHA (e.g., `1.0.3+abc1234`), convert it to a Debian-friendly format (e.g., `1.0.3-abc1234` or just `1.0.3`).
3. Cross-compile the Go binary using:
   - For `arm64`: `GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build ...`
   - For `armhf`: `GOOS=linux GOARCH=arm GOARM=7 CGO_ENABLED=0 go build ...`
4. Copy the compiled binary, the default config file, and the systemd unit file to the `dist/data/` structure.
5. Copy/generate `control` and maintainer scripts into the `dist/control/` structure. Ensure the scripts have `0755` permissions.

### Phase 3: Archive Packaging on macOS
The script will:
1. Create `debian-binary` containing `2.0\n`.
2. Generate `control.tar.gz` and `data.tar.gz` archives:
   - We must use `COPYFILE_DISABLE=1` to prevent macOS `tar` from writing resource fork (`._`) files into the archives.
   - Command: `COPYFILE_DISABLE=1 tar -czf ../control.tar.gz -C control .`
   - Command: `COPYFILE_DISABLE=1 tar -czf ../data.tar.gz -C data .`
3. Combine them into the final `.deb` package using `ar` in the precise order:
   - Command: `ar rcs monstermq-edge_<VERSION>_<DEB_ARCH>.deb debian-binary control.tar.gz data.tar.gz`
4. Validate the resulting `.deb` package.

### Phase 4: Integration
1. Add `deb-arm64`, `deb-armv7`, `deb-amd64`, and `deb-all` targets to the root `Makefile`.
2. Create a top-level `make.sh` wrapper script that defaults to building the native binary for the current machine, and supports `--deb` to build all Debian packages.
3. Document the packaging procedure in the `README.md` or a new packaging guide.

---


## 5. Open Decisions & Constraints
- **Target OS Compatibility**: The resulting binary is built as a static executable (`CGO_ENABLED=0`), which means it runs on any Linux distribution (Alpine, Debian, Ubuntu, etc.) with the correct architecture. The systemd integration assumes a systemd-based OS (like standard Raspberry Pi OS or Ubuntu Server).
- **Default Port Configuration**: The default `config.yaml` exposes certain ports (MQTT: 1883, WebSocket, GraphQL, etc.). We should ensure these do not conflict with existing services on the Pi.
- **Maintainer Info**: Should we parameterize the maintainer info or hardcode the project defaults? (We will use environment variables in the script with sensible defaults).
