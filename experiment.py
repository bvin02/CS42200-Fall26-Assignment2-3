#!/usr/bin/env python3
"""CS 422 Assignments 2 and 3 experiment and reporting pipeline."""
import argparse
import csv
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import platform
import random
import select
import socket
import statistics
import struct
import subprocess
import sys
import textwrap
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parent
SERVER_API = 'https://iperf3serverlist.net/api/servers'
REPOSITORY = 'https://github.com/bvin02/CS42200-Fall26-Assignment2-3'
COOKIE_SIZE = 37
PARAM_EXCHANGE, CREATE_STREAMS = 9, 10
TEST_START, TEST_RUNNING, TEST_END = 1, 2, 4
EXCHANGE_RESULTS, DISPLAY_RESULTS, IPERF_DONE = 13, 14, 16
ACCESS_DENIED, SERVER_ERROR = -1, -2
TCP_CONGESTION = getattr(socket, 'TCP_CONGESTION', 13)
TCP_INFO = getattr(socket, 'TCP_INFO', 11)


def now():
    return datetime.now(timezone.utc).isoformat()


def save_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(data, indent=2) + '\n')
    temp.replace(path)


def recv_exact(sock, count):
    data = bytearray()
    while len(data) < count:
        part = sock.recv(count - len(data))
        if not part:
            raise ConnectionError('peer closed the connection')
        data.extend(part)
    return bytes(data)


def send_json(sock, value):
    payload = json.dumps(value, separators=(',', ':')).encode()
    sock.sendall(struct.pack('!I', len(payload)) + payload)


def recv_json(sock, limit=16 * 1024 * 1024):
    size = struct.unpack('!I', recv_exact(sock, 4))[0]
    if size <= 0 or size > limit:
        raise ValueError(f'invalid iperf3 JSON size {size}')
    return json.loads(recv_exact(sock, size))


def recv_state(sock):
    state = struct.unpack('b', recv_exact(sock, 1))[0]
    if state == ACCESS_DENIED:
        raise ConnectionRefusedError('iperf3 server is busy or denied access')
    if state == SERVER_ERROR:
        code, system_error = struct.unpack('!II', recv_exact(sock, 8))
        raise RuntimeError(f'iperf3 server error {code}, errno {system_error}')
    if state < 0:
        raise RuntimeError(f'iperf3 returned state {state}')
    return state


def expect_state(sock, expected):
    actual = recv_state(sock)
    if actual != expected:
        raise RuntimeError(f'expected iperf3 state {expected}, received {actual}')


def tcp_info(sock):
    """Decode Linux 4.19+ struct tcp_info using stable UAPI offsets."""
    raw = sock.getsockopt(socket.IPPROTO_TCP, TCP_INFO, 232)
    if len(raw) < 144:
        raise OSError(f'TCP_INFO returned only {len(raw)} bytes')
    u32 = lambda offset: struct.unpack_from('=I', raw, offset)[0]
    u64 = lambda offset: struct.unpack_from('=Q', raw, offset)[0]
    data = dict(
        snd_mss=u32(16), unacked=u32(24), lost=u32(32), retrans=u32(36),
        rtt_us=u32(68), rttvar_us=u32(72), snd_ssthresh=u32(76),
        snd_cwnd_segments=u32(80), total_retrans=u32(100),
        pacing_rate_Bps=u64(104) if len(raw) >= 112 else None,
        bytes_acked=u64(120) if len(raw) >= 128 else None,
        segs_out=u32(136) if len(raw) >= 140 else None,
        delivery_rate_Bps=u64(160) if len(raw) >= 168 else None,
        delivered=u32(192) if len(raw) >= 196 else None,
        bytes_sent=u64(200) if len(raw) >= 208 else None,
        bytes_retrans=u64(208) if len(raw) >= 216 else None,
    )
    data['snd_cwnd_bytes'] = data['snd_cwnd_segments'] * data['snd_mss']
    return data


def set_algorithm(sock, requested):
    sock.setsockopt(socket.IPPROTO_TCP, TCP_CONGESTION, requested.encode())
    return sock.getsockopt(socket.IPPROTO_TCP, TCP_CONGESTION, 64).split(b'\0', 1)[0].decode()


def available_algorithms():
    path = Path('/proc/sys/net/ipv4/tcp_available_congestion_control')
    return path.read_text().split() if path.exists() else []


def make_cookie():
    value = str(uuid.uuid4()).encode() + b'\0'
    assert len(value) == COOKIE_SIZE
    return value


def parameter_message(duration, block_size, algorithm):
    return dict(tcp=True, omit=0, time=duration, num=0, blockcount=0,
                parallel=1, len=block_size, congestion=algorithm,
                congestion_used=algorithm, client_version='3.20')


def result_message(bytes_sent, elapsed, retransmits, algorithm):
    return dict(cpu_util_total=0.0, cpu_util_user=0.0, cpu_util_system=0.0,
                sender_has_retransmits=1, congestion_used=algorithm,
                streams=[dict(id=1, bytes=bytes_sent, retransmits=retransmits,
                              jitter=0.0, errors=0, omitted_errors=0,
                              packets=0, omitted_packets=0,
                              start_time=0.0, end_time=elapsed)])


def take_sample(sock, run, elapsed, previous, bytes_written):
    info = tcp_info(sock)
    acknowledged = info['bytes_acked']
    if acknowledged is None:
        acknowledged = bytes_written - info['unacked'] * info['snd_mss']
    delta_t = elapsed - previous['elapsed']
    delta_ack = max(0, acknowledged - previous['bytes_acked'])
    total_retrans = info['total_retrans']
    row = dict(run_id=run['run_id'], destination=run['host'], port=run['port'],
               requested_algorithm=run['requested_algorithm'], algorithm=run['algorithm'],
               repetition=run['repetition'], timestamp_utc=now(), elapsed_s=elapsed,
               interval_s=delta_t, bytes_written=bytes_written,
               bytes_acked=acknowledged, acked_interval_bytes=delta_ack,
               goodput_bps=(delta_ack * 8 / delta_t) if delta_t > 0 else 0.0,
               snd_cwnd_segments=info['snd_cwnd_segments'],
               snd_mss_bytes=info['snd_mss'], snd_cwnd_bytes=info['snd_cwnd_bytes'],
               srtt_us=info['rtt_us'], rttvar_us=info['rttvar_us'],
               lost=info['lost'], retrans=info['retrans'], total_retrans=total_retrans,
               retrans_interval=max(0, total_retrans - previous['total_retrans']),
               unacked=info['unacked'], snd_ssthresh=info['snd_ssthresh'],
               pacing_rate_Bps=info['pacing_rate_Bps'],
               delivery_rate_Bps=info['delivery_rate_Bps'],
               bytes_retrans=info['bytes_retrans'], delivered=info['delivered'])
    return row, dict(elapsed=elapsed, bytes_acked=acknowledged,
                     total_retrans=total_retrans)


def run_iperf3(host, port, algorithm, duration, interval, timeout, block_size,
               repetition, run_id):
    """Run one forward TCP test using the iperf3 wire protocol, not its binary."""
    run = dict(run_id=run_id, host=host, port=port,
               requested_algorithm=algorithm, algorithm=None,
               repetition=repetition, started=now(), status='starting')
    control = data = None
    samples = []
    started = None
    bytes_written = 0
    try:
        cookie = make_cookie()
        control = socket.create_connection((host, port), timeout=timeout)
        control.settimeout(timeout)
        control.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        control.sendall(cookie)
        expect_state(control, PARAM_EXCHANGE)

        data = socket.socket(control.family, socket.SOCK_STREAM)
        data.settimeout(timeout)
        actual = set_algorithm(data, algorithm)
        run['algorithm'] = actual
        send_json(control, parameter_message(duration, block_size, actual))
        expect_state(control, CREATE_STREAMS)
        data.connect((host, port))
        data.sendall(cookie)
        expect_state(control, TEST_START)
        expect_state(control, TEST_RUNNING)

        data.setblocking(False)
        payload = memoryview(os.urandom(block_size))
        started = time.monotonic()
        first = tcp_info(data)
        first_acked = first['bytes_acked'] or 0
        previous = dict(elapsed=0.0, bytes_acked=first_acked,
                        total_retrans=first['total_retrans'])
        next_sample = interval
        while True:
            elapsed = time.monotonic() - started
            if elapsed >= duration:
                break
            wait = min(0.05, max(0.0, next_sample - elapsed), duration - elapsed)
            _, writable, _ = select.select([], [data], [], wait)
            if writable:
                try:
                    bytes_written += data.send(payload)
                except BlockingIOError:
                    pass
            elapsed = time.monotonic() - started
            if elapsed >= next_sample:
                row, previous = take_sample(data, run, elapsed, previous, bytes_written)
                samples.append(row)
                next_sample += interval

        elapsed = time.monotonic() - started
        if not samples or elapsed - samples[-1]['elapsed_s'] > interval * .25:
            row, previous = take_sample(data, run, elapsed, previous, bytes_written)
            samples.append(row)
        final_retrans = previous['total_retrans']
        control.sendall(struct.pack('b', TEST_END))
        data.setblocking(True)
        try:
            data.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        expect_state(control, EXCHANGE_RESULTS)
        send_json(control, result_message(bytes_written, elapsed, final_retrans, actual))
        run['server_results'] = recv_json(control)
        expect_state(control, DISPLAY_RESULTS)
        control.sendall(struct.pack('b', IPERF_DONE))
        run.update(status='complete', finished=now(), elapsed_s=elapsed,
                   bytes_written=bytes_written, samples=len(samples),
                   total_retrans=final_retrans)
    except Exception as exc:
        run.update(status='failed', finished=now(), error=f'{type(exc).__name__}: {exc}',
                   elapsed_s=(time.monotonic() - started) if started else None,
                   bytes_written=bytes_written, samples=len(samples))
    finally:
        for stream in (data, control):
            if stream:
                try:
                    stream.close()
                except OSError:
                    pass
    return run, samples


def parse_port(value, rng):
    text = str(value).strip()
    if '-' in text:
        low, high = map(int, text.split('-', 1))
        return rng.randint(low, high)
    return int(text)


def fetch_servers(path):
    request = urllib.request.Request(SERVER_API, headers={'User-Agent': 'CS422-Assignments2-3/1.0'})
    with urllib.request.urlopen(request, timeout=20) as response:
        data = json.load(response)
    if not isinstance(data, list) or not data:
        raise ValueError('server API returned no entries')
    save_json(path, dict(source=SERVER_API, retrieved=now(), servers=data))
    return data


def read_servers(path, seed):
    path = Path(path)
    if path.suffix.lower() == '.json':
        raw = json.loads(path.read_text())
        rows = raw.get('servers', raw) if isinstance(raw, dict) else raw
    else:
        rows = []
        for line_number, line in enumerate(path.read_text().splitlines(), 1):
            line = line.split('#', 1)[0].strip()
            if not line:
                continue
            host, port = line.rsplit(':', 1)
            rows.append(dict(ip=host.strip('[]'), port=port, source_line=line_number))
    rng = random.Random(seed)
    endpoints = []
    for row in rows:
        try:
            endpoint = dict(host=str(row['ip']).strip(), port=parse_port(row['port'], rng),
                            location=row.get('location'), country=row.get('country'),
                            provider=row.get('provider'))
            if endpoint['host'] and 0 < endpoint['port'] < 65536:
                endpoints.append(endpoint)
        except (KeyError, TypeError, ValueError):
            continue
    rng.shuffle(endpoints)
    if not endpoints:
        raise ValueError('server list contains no valid host:port entries')
    return endpoints


def write_csv(path, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0]) if rows else []
    with path.open('w', newline='') as stream:
        if fields:
            writer = csv.DictWriter(stream, fields, extrasaction='ignore', lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)


def collect(args):
    if platform.system() != 'Linux':
        raise OSError('measurement collection requires Linux; use ./run.sh (Docker)')
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    source = Path(args.servers)
    if args.fetch or not source.exists():
        fetch_servers(source)
    endpoints = read_servers(source, args.seed)
    requested = list(dict.fromkeys(x.strip() for x in args.algorithms.split(',') if x.strip()))
    available = available_algorithms()
    metadata = dict(started=now(), platform=platform.platform(), kernel=platform.release(),
                    python=sys.version, server_source=str(source), api=SERVER_API,
                    available_algorithms=available, requested_algorithms=requested,
                    config={k: v for k, v in vars(args).items() if k != 'func'},
                    repository=REPOSITORY, tcp_info=dict(length=232,
                    layout='Linux include/uapi/linux/tcp.h offsets; microseconds for srtt/rttvar, bytes for cwnd and counters'))
    save_json(out / 'metadata.json', metadata)
    runs, samples, selected = [], [], []
    complete_destinations = 0
    attempts = 0
    candidate = 0
    max_attempts = min(len(endpoints), args.max_attempts)
    while complete_destinations < args.destinations and attempts < max_attempts:
        endpoint = endpoints[candidate]
        candidate += 1
        attempts += 1
        destination_complete = False
        print(f'[{attempts}/{max_attempts}] {endpoint["host"]}:{endpoint["port"]}', flush=True)
        endpoint_runs = []
        for repetition in range(1, args.repetitions + 1):
            order = requested[:]
            random.Random(args.seed + attempts * 1000 + repetition).shuffle(order)
            for algorithm in order:
                run_id = (f'd{complete_destinations + 1:02d}-a{attempts:02d}-'
                          f'r{repetition:02d}-{algorithm}')
                if algorithm not in available:
                    run = dict(run_id=run_id, host=endpoint['host'], port=endpoint['port'],
                               requested_algorithm=algorithm, algorithm=None,
                               repetition=repetition, started=now(), finished=now(),
                               status='unsupported', error=f'{algorithm} not in {available}', samples=0)
                    rows = []
                else:
                    print(f'  repetition {repetition}, {algorithm}', flush=True)
                    run, rows = run_iperf3(endpoint['host'], endpoint['port'], algorithm,
                                           args.duration, args.interval, args.timeout,
                                           args.block_size, repetition, run_id)
                endpoint_runs.append(run)
                runs.append(run)
                samples.extend(rows)
                save_json(out / 'runs.json', runs)
                write_csv(out / 'samples.csv', samples)
                if run['status'] == 'failed':
                    print('   ', run['error'], flush=True)
        supported = [name for name in requested if name in available]
        destination_complete = bool(supported) and all(
            any(r['status'] == 'complete' and r['requested_algorithm'] == name
                for r in endpoint_runs) for name in supported)
        if destination_complete:
            selected.append(endpoint)
            complete_destinations += 1
        else:
            print('  no successful run; trying a replacement server', flush=True)
    metadata.update(finished=now(), attempts=attempts, selected_destinations=selected,
                    completed_destinations=complete_destinations,
                    destination_shortfall=max(0, args.destinations - complete_destinations))
    save_json(out / 'metadata.json', metadata)
    render_results(out)
    return 0 if complete_destinations else 2


def percentile(values, p):
    values = sorted(values)
    if not values:
        return math.nan
    position = (len(values) - 1) * p
    low = math.floor(position)
    high = math.ceil(position)
    return values[low] if low == high else values[low] + (values[high] - values[low]) * (position - low)


def summarize(samples):
    groups = {}
    for row in samples:
        key = (row['destination'], int(row['port']), row['algorithm'])
        groups.setdefault(key, []).append(float(row['goodput_bps']))
    result = []
    for (host, port, algorithm), values in sorted(groups.items()):
        result.append(dict(destination=host, port=port, algorithm=algorithm, samples=len(values),
                           minimum_Mbps=min(values) / 1e6, median_Mbps=statistics.median(values) / 1e6,
                           mean_Mbps=statistics.mean(values) / 1e6,
                           p95_Mbps=percentile(values, .95) / 1e6))
    return result


def load_csv(path):
    if not path.exists() or not path.read_text().strip():
        return []
    with path.open(newline='') as stream:
        return list(csv.DictReader(stream))


def render_results(out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    out = Path(out)
    metadata = json.loads((out / 'metadata.json').read_text())
    runs = json.loads((out / 'runs.json').read_text()) if (out / 'runs.json').exists() else []
    samples = load_csv(out / 'samples.csv')
    selected = {(row['host'], int(row['port']))
                for row in metadata.get('selected_destinations', [])}
    complete_ids = {r['run_id'] for r in runs
                    if r['status'] == 'complete' and
                    (r['host'], int(r['port'])) in selected}
    completed_samples = [row for row in samples if row['run_id'] in complete_ids]
    summary = summarize(completed_samples)
    write_csv(out / 'summary.csv', summary)
    plots = out / 'plots'
    plots.mkdir(exist_ok=True)
    plt.rcParams.update({'font.size': 9, 'axes.spines.top': False,
                         'axes.spines.right': False, 'axes.unicode_minus': False})

    selected = {(row['host'], int(row['port']))
                for row in metadata.get('selected_destinations', [])}
    complete = [r for r in runs if r['status'] == 'complete' and
                (r['host'], int(r['port'])) in selected]
    representative = complete[0]['host'] if complete else None
    representative_rows = [r for r in completed_samples if r['destination'] == representative]
    algorithms = metadata['requested_algorithms']
    colors = {name: plt.get_cmap('tab10')(i) for i, name in enumerate(algorithms)}
    figures = []

    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5), layout='constrained')
    metrics = [('goodput_bps', 'Goodput (Mbps)', 1e6), ('snd_cwnd_bytes', 'snd_cwnd (MB)', 1e6),
               ('srtt_us', 'SRTT (ms)', 1e3), ('total_retrans', 'Cumulative retransmissions', 1)]
    for ax, (key, ylabel, scale) in zip(axes.flat, metrics):
        for algorithm in algorithms:
            rows = [r for r in representative_rows if r['algorithm'] == algorithm]
            by_run = {}
            for row in rows:
                by_run.setdefault(row['run_id'], []).append(row)
            for run_id, values in by_run.items():
                ax.plot([float(x['elapsed_s']) for x in values],
                        [float(x[key]) / scale for x in values],
                        color=colors[algorithm], alpha=.65,
                        label=algorithm if run_id == next(iter(by_run)) else None)
        ax.set(xlabel='Elapsed time (s)', ylabel=ylabel)
        ax.grid(alpha=.2)
        if ax.lines:
            ax.legend()
    fig.suptitle(f'TCP time series - {representative or "no completed destination"}')
    figures.append(('time_series', fig))

    fig, axes = plt.subplots(1, 3, figsize=(12, 4), layout='constrained')
    scatters = [('snd_cwnd_bytes', 'snd_cwnd (MB)', 1e6), ('srtt_us', 'SRTT (ms)', 1e3),
                ('retrans_interval', 'Interval retransmissions', 1)]
    for ax, (key, xlabel, scale) in zip(axes, scatters):
        for algorithm in algorithms:
            rows = [r for r in representative_rows if r['algorithm'] == algorithm]
            if rows:
                ax.scatter([float(x[key]) / scale for x in rows],
                           [float(x['goodput_bps']) / 1e6 for x in rows],
                           s=14, alpha=.55, color=colors[algorithm], label=algorithm)
        ax.set(xlabel=xlabel, ylabel='Goodput (Mbps)')
        ax.grid(alpha=.2)
        if ax.collections:
            ax.legend()
    fig.suptitle(f'TCP metric relationships - {representative or "no completed destination"}')
    figures.append(('scatter', fig))

    for name, figure in figures:
        figure.savefig(plots / f'{name}.pdf', bbox_inches='tight')
        figure.savefig(plots / f'{name}.png', dpi=170, bbox_inches='tight')

    sections = report_sections(metadata, runs, completed_samples, summary, representative)
    markdown = ['# Assignments 2 and 3: TCP Congestion Control', '']
    for title, paragraphs in sections:
        markdown.extend(['## ' + title, '', '\n\n'.join(paragraphs), ''])
    for name, _ in figures:
        markdown.extend([f'![{name}](plots/{name}.png)', ''])
    (out / 'report.md').write_text('\n'.join(markdown))
    with PdfPages(out / 'report.pdf') as pdf:
        page = 1
        for title, paragraphs in sections:
            page = text_pages(pdf, title, paragraphs, plt, page)
        for _, figure in figures:
            figure.text(.5, .015, str(page), ha='center', fontsize=8, color='#555555')
            pdf.savefig(figure)
            page += 1
        if summary:
            for offset in range(0, len(summary), 25):
                fig, ax = plt.subplots(figsize=(11.7, 8.3)); ax.axis('off')
                ax.set_title('Goodput summary', loc='left', pad=20)
                rows = [[x['destination'], x['port'], x['algorithm'], x['samples'],
                         f"{x['minimum_Mbps']:.2f}", f"{x['median_Mbps']:.2f}",
                         f"{x['mean_Mbps']:.2f}", f"{x['p95_Mbps']:.2f}"]
                        for x in summary[offset:offset + 25]]
                table = ax.table(cellText=rows, colLabels=['Destination','Port','CC','N','Min','Median','Mean','P95 Mbps'],
                                 loc='upper center', colWidths=[.25,.08,.09,.06,.1,.1,.1,.1])
                table.auto_set_font_size(False); table.set_fontsize(8); table.scale(1, 1.35)
                fig.text(.5, .015, str(page), ha='center', fontsize=8, color='#555555')
                pdf.savefig(fig); plt.close(fig); page += 1
    for _, figure in figures:
        plt.close(figure)
    print(f'Created {out}/report.pdf and plots.', flush=True)


def text_pages(pdf, title, paragraphs, plt, page):
    lines = []
    for paragraph in paragraphs:
        lines.extend(textwrap.wrap(paragraph, 96, break_long_words=True))
        lines.append('')
    for start in range(0, max(1, len(lines)), 43):
        fig = plt.figure(figsize=(8.27, 11.69))
        fig.text(.08, .94, title + (' (continued)' if start else ''), fontsize=17,
                 weight='bold', va='top')
        fig.text(.08, .885, '\n'.join(lines[start:start + 43]), fontsize=10,
                 va='top', linespacing=1.5)
        fig.text(.5, .015, str(page), ha='center', fontsize=8, color='#555555')
        pdf.savefig(fig); plt.close(fig); page += 1
    return page


def report_sections(metadata, runs, samples, summary, representative):
    config = metadata['config']
    selected = {(row['host'], int(row['port']))
                for row in metadata.get('selected_destinations', [])}
    complete = [r for r in runs if r['status'] == 'complete' and
                (r['host'], int(r['port'])) in selected]
    failed = [r for r in runs if r['status'] == 'failed']
    unsupported = [r for r in runs if r['status'] == 'unsupported']
    completed_algorithms = sorted({r['algorithm'] for r in complete})
    available = ', '.join(metadata['available_algorithms']) or 'none reported'
    limitations = []
    if unsupported:
        limitations.append('The measurement host did not expose: ' + ', '.join(sorted({r['requested_algorithm'] for r in unsupported})) + '.')
    if metadata.get('destination_shortfall'):
        limitations.append(f"The run finished {metadata['destination_shortfall']} destination(s) short after the configured bounded attempts.")
    limitations.append('Public servers, routes, cross traffic, and server load change over time, so algorithm differences are not controlled causal estimates.')
    best = max(summary, key=lambda x: x['mean_Mbps']) if summary else None
    measured = {}
    for row in samples:
        values = measured.setdefault(row['algorithm'], dict(goodput=[], rtt=[], retrans=[]))
        values['goodput'].append(float(row['goodput_bps']) / 1e6)
        values['rtt'].append(float(row['srtt_us']) / 1e3)
        values['retrans'].append(float(row['retrans_interval']))
    comparison = []
    for algorithm, values in sorted(measured.items()):
        comparison.append(
            f"{algorithm} averaged {statistics.mean(values['goodput']):.2f} Mbps, "
            f"had {statistics.median(values['rtt']):.2f} ms median SRTT, and logged "
            f"{sum(values['retrans']):.0f} interval retransmission increments")
    per_run = {}
    for row in samples:
        per_run.setdefault((row['run_id'], row['algorithm']), []).append(float(row['goodput_bps']) / 1e6)
    ranges = {}
    for (_, algorithm), values in per_run.items():
        ranges.setdefault(algorithm, []).append(statistics.mean(values))
    variability = '; '.join(
        f'{algorithm} run means ranged from {min(values):.2f} to {max(values):.2f} Mbps'
        for algorithm, values in sorted(ranges.items())) or 'No completed run means were available'
    four_way = all(name in measured for name in ('cubic', 'reno', 'bbr', 'bhavin'))
    completed_counts = {name: sum(r['algorithm'] == name for r in complete)
                        for name in ('cubic', 'reno', 'bbr', 'bhavin')}
    observations = ('The largest observed mean goodput was '
                    f"{best['mean_Mbps']:.2f} Mbps for {best['algorithm']} to {best['destination']}. "
                    if best else 'No test completed, so measurement-based comparisons are unavailable. ')
    observations += ((' Across all accepted samples, ' + '; '.join(comparison) + '. ') if comparison else ' ')
    observations += ('A larger congestion window permits more outstanding bytes, but useful rate is limited by the path bandwidth-delay product. '
                     'Higher RTT delays feedback; rising RTT commonly indicates queueing. Retransmission increments are a loss proxy and can coincide with a window reduction. '
                     'Zero retransmissions do not prove zero congestion because ECN, application limits, and receiver limits may also matter. '
                     f'Across repetitions, {variability}.')
    design = ('BhavinCC starts with the kernel initial congestion window and tracks minimum RTT, an RTT EWMA, delivery throughput, and recent loss. '
              'On each ACK: compute queue_ratio=(srtt-min_rtt)/min_rtt. If loss occurred, set cwnd=max(4,0.70*cwnd). '
              'Else if queue_ratio>0.25 and delivered throughput did not improve, reduce cwnd by max(1,cwnd/16). '
              'Else if queue_ratio<0.10, increase cwnd by approximately one segment per RTT; in the middle region increase more slowly. '
              'The included tcp_bhavin.c implements these rules in Linux congestion-control callbacks. The 0.70 multiplicative decrease yields convergence under persistent loss, '
              'while RTT-gradient avoidance reduces standing queues. RTT unfairness can remain because shorter-RTT flows receive ACK feedback sooner; noisy wireless loss and route changes can also cause false reductions.')
    a3 = ('Assignment 3 uses Option 1 (kernel implementation). The module registers the name bhavin, records its selection through TCP_CONGESTION, and shares the same client, logging, and plotting path as the baselines. '
          'From the kernel directory, build with make, load with sudo insmod tcp_bhavin.ko, and unload with sudo rmmod tcp_bhavin. '
          'A matching /lib/modules kernel build tree, CONFIG_MODULES, and root privileges are required. '
          + (f'For this experiment, BhavinCC compiled against and loaded into the running {metadata["kernel"]} kernel; tcp_bbr was also loaded, and both names were verified through TCP_CONGESTION before collection.'
             if four_way else
             'Docker Desktop does not supply matching LinuxKit module headers, so module loading and four-way collection require native Ubuntu/WSL2 configured for external modules.'))
    ai = ('AI assistance was used to inspect the published iperf3 protocol state machine, draft code and tests, and check report completeness. '
          'The implementation was verified against automated unit/integration tests and the official protocol behavior; I reviewed the code and remain responsible for explaining it.')
    source_lines = (ROOT / 'experiment.py').read_text().splitlines()
    def code_link(name):
        line = next(i for i, value in enumerate(source_lines, 1)
                    if value.startswith('def ' + name + '('))
        return f'{REPOSITORY}/blob/main/experiment.py#L{line}'
    kernel_lines = (ROOT / 'kernel' / 'tcp_bhavin.c').read_text().splitlines()
    kernel_line = next(i for i, value in enumerate(kernel_lines, 1)
                       if value.startswith('static void bhavin_cong_avoid'))
    return [
        ('Assignments 2 and 3: TCP Congestion Control', [
            'Bhavin Gupta | CS 422: Computer Networks | Fall 2026',
            'Repository: ' + REPOSITORY,
            f"Host kernel: {metadata['kernel']}. Available congestion controls: {available}. Completed runs: {len(complete)}; failed runs: {len(failed)}.",
            f"Configuration: {config['destinations']} destinations, {config['duration']} seconds, {config['interval']} second sampling, {config['repetitions']} repetition(s), seed {config['seed']}. Algorithms were shuffled within each destination/repetition; the exact order is in runs.json.",
            ('Reproduce on the configured native Linux host with: python3 experiment.py collect --servers inputs/servers_us.txt --output results --destinations 3 --duration 10 --interval 1 --repetitions 2 --algorithms cubic,reno,bbr,bhavin.'
             if four_way else
             'Reproduce with: ./run.sh collect --fetch --destinations 10 --duration 60 --interval 1 --repetitions 3 --algorithms cubic,reno,bbr,bhavin.'), ai]),
        ('Assignment 2 - implementation and measurements', [
            'The Python client creates an iperf3 control socket, sends a 37-byte cookie, handles PARAM_EXCHANGE, sends length-prefixed JSON parameters, opens a separate data socket with the chosen TCP_CONGESTION value, transmits data, sends TEST_END, exchanges result JSON, and finishes with IPERF_DONE. It never invokes the iperf3 executable.',
            'Each sample comes from getsockopt(TCP_INFO) on the data socket. Goodput is 8 times the increase in cumulative bytes_acked divided by the actual monotonic elapsed interval. snd_cwnd is converted from segments to bytes using snd_mss. SRTT and rttvar are kernel microseconds. total_retrans is cumulative; retrans_interval is its nonnegative interval delta.',
            f"The representative destination used in the figures is {representative or 'unavailable'}. Completed algorithms were {', '.join(completed_algorithms) or 'none'}.", observations,
            ('CUBIC grows its window as a cubic function of time since congestion and is intended to scale on large bandwidth-delay products. Reno uses additive increase and multiplicative decrease and is often competitive on smaller BDP paths but reacts conservatively to loss. BBR estimates bottleneck bandwidth and minimum RTT, so it can perform well on large-BDP or non-congestion-loss paths without filling a large buffer. '
             + ('This run includes live BBR measurements on the same destinations and schedule as the other algorithms.' if 'bbr' in measured else 'This host supplied no BBR measurements, so the BBR regime statement is analytical rather than empirical for this run.')),
            'Limitations: ' + ' '.join(limitations)]),
        ('Assignment 2 - hand-written congestion-control design', [design]),
        ('Assignment 3 - Option 1 implementation and evaluation', [a3,
            (f'The live comparison completed {completed_counts["cubic"]} CUBIC, {completed_counts["reno"]} Reno, {completed_counts["bbr"]} BBR, and {completed_counts["bhavin"]} BhavinCC runs on the accepted destinations. The figures and aggregates use only successful runs; public-server failures remain in runs.json.'
             if four_way else
             'The same-duration, same-interval comparison is automated for cubic, reno, bbr, and bhavin. Unsupported controls are explicitly logged rather than replaced.')]),
        ('Files and verification', [
            f"Core protocol and TCP_INFO code: experiment.py. Custom kernel algorithm: kernel/tcp_bhavin.c. Automated tests: tests/test_experiment.py. Server input: {metadata['server_source']}. Raw status, samples, aggregates, and environment evidence are stored together in this output directory. Plot-generating function: render_results in experiment.py.",
            ('Code links: iperf3 state machine ' + code_link('run_iperf3') + '; TCP_INFO decoding ' + code_link('tcp_info') +
             '; collection pipeline ' + code_link('collect') + '; plot/report generation ' + code_link('render_results') +
             f'; BhavinCC window update {REPOSITORY}/blob/main/kernel/tcp_bhavin.c#L{kernel_line}.'),
            'The percentile method is linear interpolation at rank (n-1)*0.95 after sorting. All rates use decimal SI units (1 Mbps = 1,000,000 bits/s).'])]


def render_command(args):
    render_results(Path(args.output))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    collect_parser = sub.add_parser('collect', help='run measurements and render the report')
    collect_parser.add_argument('--servers', default='inputs/servers.json')
    collect_parser.add_argument('--output', default='results')
    collect_parser.add_argument('--fetch', action='store_true')
    collect_parser.add_argument('--destinations', type=int, default=10)
    collect_parser.add_argument('--duration', type=float, default=60)
    collect_parser.add_argument('--interval', type=float, default=1)
    collect_parser.add_argument('--repetitions', type=int, default=3)
    collect_parser.add_argument('--algorithms', default='cubic,reno,bbr,bhavin')
    collect_parser.add_argument('--max-attempts', type=int, default=40)
    collect_parser.add_argument('--timeout', type=float, default=10)
    collect_parser.add_argument('--block-size', type=int, default=128 * 1024)
    collect_parser.add_argument('--seed', type=int, default=422)
    collect_parser.set_defaults(func=collect)
    render_parser = sub.add_parser('render', help='regenerate plots/report from saved data')
    render_parser.add_argument('--output', default='results')
    render_parser.set_defaults(func=render_command)
    args = parser.parse_args()
    if args.action == 'collect':
        for key in ('destinations', 'duration', 'interval', 'repetitions', 'max_attempts', 'timeout', 'block_size'):
            if getattr(args, key) <= 0:
                parser.error('--' + key.replace('_', '-') + ' must be positive')
        if args.interval > args.duration:
            parser.error('--interval cannot exceed --duration')
    try:
        result = args.func(args)
        raise SystemExit(result or 0)
    except (ValueError, OSError) as exc:
        parser.exit(1, f'Error: {exc}\n')


if __name__ == '__main__':
    main()
