# CS 422 - Assignments 2 and 3

Bhavin Gupta  
Repository: https://github.com/bvin02/CS42200-Fall26-Assignment2-3

This repository contains a from-scratch Python iperf3-compatible TCP client,
Linux `TCP_INFO` tracing, automatic plots/reports, and the BhavinCC Linux kernel
congestion-control implementation. The client does not run the iperf3 binary.

## Assignment 2

Run the full experiment on native Ubuntu 22.04+ or WSL2 after loading the
required congestion-control modules. Docker standardizes user space, but the
algorithms and `TCP_INFO` fields still come from the host kernel.

```bash
python3 experiment.py collect --servers inputs/servers_us.txt --output results \
  --destinations 3 --duration 10 --interval 1 --repetitions 2 \
  --algorithms cubic,reno,bbr,bhavin
```

For a short protocol check:

```bash
python3 experiment.py collect --servers inputs/servers_us.txt --output results \
  --destinations 1 --duration 3 --interval 1 --repetitions 1 \
  --algorithms bbr,bhavin --max-attempts 10
```

The server list accepts JSON from the public API or text with one `host:port`
per line. Port ranges in the public list are resolved reproducibly from `--seed`.
Failed/busy servers are logged and replaced up to `--max-attempts`. Unsupported
algorithms are recorded rather than silently changed.

## Assignment 3 - Option 1

BhavinCC is in `kernel/tcp_bhavin.c`. Build and load it on the same supported
Linux host whose kernel headers match the running kernel:

```bash
sudo apt install build-essential linux-headers-$(uname -r)
make -C kernel
sudo insmod kernel/tcp_bhavin.ko
sysctl net.ipv4.tcp_available_congestion_control
```

Enable BBR, load BhavinCC, verify both names, and run the normal pipeline:

```bash
sudo modprobe tcp_bbr
cat /proc/sys/net/ipv4/tcp_available_congestion_control
python3 experiment.py collect --servers inputs/servers_us.txt --output results \
  --destinations 3 --duration 10 --interval 1 --repetitions 2 \
  --algorithms cubic,reno,bbr,bhavin
sudo rmmod tcp_bhavin
```

Docker Desktop uses a LinuxKit host kernel. It is useful for validating the
client, CUBIC, and Reno, but it normally lacks BBR and the matching build tree
needed to load an external module. Use native Ubuntu/WSL2 for the required
four-algorithm Assignment 3 run.

## Output and tests

- `results/report.pdf` - submission report
- `results/runs.json` - run order, status, errors, and server results
- `results/samples.csv` - interval TCP measurements
- `results/summary.csv` - min/median/mean/95th-percentile goodput
- `results/plots/` - PDF and PNG time-series/scatter figures

```bash
docker build -t cs422-a2-a3 .
docker run --rm --entrypoint python3 cs422-a2-a3 \
  -m unittest discover -s tests -v
```

The committed results were collected on Ubuntu 24.04.5 with kernel
`6.8.0-147-generic`. Matching headers were used to compile and load BhavinCC,
and `tcp_bbr` was loaded before collection. CUBIC, Reno, BBR, and BhavinCC were
each verified through `TCP_CONGESTION` on the data socket. BBR and BhavinCC each
completed six live runs with 60 TCP_INFO samples. See `results/environment.txt`
for kernel/module evidence and `results/validation.txt` for data assertions.

AI assistance was used to inspect the published iperf3 state machine, draft
code and tests, and check report completeness. I reviewed the implementation
and am responsible for explaining it during evaluation.
