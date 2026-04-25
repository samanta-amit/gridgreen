import boto3
import paramiko
import threading, time, shlex
from subprocess import DEVNULL, STDOUT, check_call
import subprocess as sp
import random
import os, glob
import sys
import shutil
import statistics
from itertools import product
from multiprocessing import Pool, cpu_count
import numpy as np

FNULL = open(os.devnull, 'w')

# SRAsearch-specific lambda reduction list and timing constants
lam_red_list = ['bowtie2_build', 'merge1', 'merge2']
lam_red_exe  = [167.6949908733368, 175.91879749298096, 102.77057576179504]
lam_red_read = [0.12768292427062988, 0.09047198295593262, 0.14659523963928223]
lam_red_write= [0.038234710693359375, 0.030656099319458008, 0.0741269588470459]

# Carbon footprint constants
CARBON_INTENSITY_US_EAST_2 = 400  # gCO2e/kWh for us-east-2 (Ohio)
EC2_POWER = {'m5a.8xlarge': 1.80}
LAMBDA_POWER_PER_SEC = 0.005
REGIONAL_CARBON_INTENSITY = {
    'us-west-1': 200, 'us-east-1': 400, 'eu-west-1': 300,
    'ap-southeast-1': 500, 'sa-east-1': 350, 'eu-central-1': 320
}

# Costs and constraints
DATA_TRANSFER_TIME = 5
DATA_TRANSFER_COST = 0.01
MAX_COST = 10.0
S3_UPLOAD_TIME = 2
S3_DOWNLOAD_TIME = 2
WARMUP_TIME = 10
CI_THRESHOLD = 0.2
factor = 0.5

# Real queue time data
REAL_NODES = [8, 16, 32, 64, 128]
REAL_QUEUE_TIMES = [128780, 175037, 704362, 1256958, 2141369]  # in seconds

# Configuration for LAMBDA_FACTOR and EC2_THRESHOLD
config = {
    'LAMBDA_FACTOR': None,
    'EC2_THRESHOLD': None
}

def set_config(lambda_factor=None, ec2_threshold=None):
    if lambda_factor is not None:
        config['LAMBDA_FACTOR'] = lambda_factor
    if ec2_threshold is not None:
        config['EC2_THRESHOLD'] = ec2_threshold

def unset_config(param=None):
    if param is None or param == 'LAMBDA_FACTOR':
        config['LAMBDA_FACTOR'] = None
    if param is None or param == 'EC2_THRESHOLD':
        config['EC2_THRESHOLD'] = None

# Global tracking
ec2_runtime_list = []
lam_cost_list = []
phase_time = []
ec2_carbon_list = []
lam_carbon_list = []
ec2_total_time = 0
lam_total_time = 0
ec2_service_time = 0
lam_service_time = 0
ec2_app_times = {}
ec2_app_carbon = {}
lam_app_times = {}
lam_app_carbon = {}
component_carbon_log = []
actual_ci_log = {}

# Caches
ec2_cache = {}
lam_cache = {}

def predict_queue_wait_time(num_nodes):
    if num_nodes == 0:
        return 0
    return np.interp(num_nodes, REAL_NODES, REAL_QUEUE_TIMES)

def predict_ci(region, base_time):
    return REGIONAL_CARBON_INTENSITY[region]

def get_actual_ci(region):
    predicted = REGIONAL_CARBON_INTENSITY[region]
    actual = predicted * (1 + random.uniform(-0.3, 0.3))
    actual_ci_log[region] = actual
    return actual

def speculative_warmup(phase_idx):
    if phase_idx + 1 >= phase:
        return
    next_apps = app[phase_idx + 1]
    for app_name in next_apps:
        if app_name in lam_red_list:
            threading.Thread(target=run_lam, args=(app_name, 0, 'us-east-1', True, False)).start()

def run_lam(name, num, region='us-east-1', warmup=False, is_optimization=False):
    global lam_total_time, lam_service_time, lam_app_times, lam_app_carbon, lam_carbon_list
    cache_key = (name, num, region)
    if cache_key in lam_cache and not warmup and not is_optimization:
        total_exe_time, carbon, service_time = lam_cache[cache_key]
    else:
        start_time = time.time()

        if num > 0:
            if name in lam_red_list:
                # Use pre-recorded timing data for reduction functions
                idx = lam_red_list.index(name)
                write_list = [lam_red_exe[idx], lam_red_read[idx], lam_red_write[idx]]
                i = 1
                while i <= num:
                    wl = [[v + random.uniform(0, v / 100) for v in write_list]]
                    with open(f"{name}_response{i}.txt", 'w') as fh:
                        for listitem in wl:
                            fh.write('%s' % listitem)
                    i += 1
                time.sleep(sum(write_list))
            else:
                pro_list = []
                for i in range(1, num + 1):
                    command = (
                        f"aws lambda invoke --function-name run_{name} "
                        f"--region {region} "
                        f"--payload '{{\"key1\":\"{i}\"}}' {name}_response{i}.txt"
                    )
                    p = sp.Popen(shlex.split(command), stdout=sp.PIPE, stderr=sp.PIPE)
                    pro_list.append((p, time.time()))

        service_time = 0
        actual_exe_time = time.time() - start_time if num > 0 else 0
        service_time = actual_exe_time
        total_exe_time = (WARMUP_TIME + actual_exe_time if warmup else actual_exe_time) + DATA_TRANSFER_TIME + S3_DOWNLOAD_TIME

        if num > 0:
            filenames = [f"{name}_response{i}.txt" for i in range(1, num + 1)]
            with open(f"{name}_response.txt", 'w') as outfile:
                for fname in filenames:
                    try:
                        with open(fname) as infile:
                            outfile.write(infile.read() + "\n")
                    except Exception:
                        pass
            for file in filenames:
                try:
                    sp.check_output(shlex.split(f"rm {file}"))
                except Exception:
                    pass

            # Parse response and compute cost
            numlist = []
            try:
                with open(f"{name}_response.txt", "r") as rf:
                    for line in rf:
                        k = line[1:-2].split(",")
                        s = float(k[0]) + float(k[1]) + float(k[2])
                        numlist.append(s)
            except Exception:
                pass

            if numlist:
                cost = statistics.mean(numlist) * num * cost_of_lambda
            else:
                cost = total_exe_time * cost_of_lambda + S3_DOWNLOAD_TIME * num * s3_cost

            if not is_optimization:
                lam_cost_list.append(cost)

        energy_kwh = (total_exe_time * LAMBDA_POWER_PER_SEC) / 3600
        actual_ci = get_actual_ci(region)
        carbon = energy_kwh * actual_ci

        if not is_optimization:
            lam_cache[cache_key] = (total_exe_time, carbon, service_time)

    if not is_optimization:
        lam_total_time += total_exe_time
        lam_service_time += service_time
        lam_app_times[name] = lam_app_times.get(name, 0) + total_exe_time
        lam_app_carbon[name] = lam_app_carbon.get(name, 0) + carbon
        lam_carbon_list.append((name, carbon))
        component_carbon_log.append((name, 'Lambda', carbon))

    return total_exe_time, carbon, service_time

def simulate_lambda_carbon(app_name, num_tasks):
    carbon_by_region = {}
    for region in REGIONAL_CARBON_INTENSITY:
        total_exe_time, carbon, service_time = run_lam(app_name, num_tasks, region, False, True)
        carbon_by_region[region] = {
            'total_time': total_exe_time,
            'service_time': service_time,
            'carbon': carbon,
            'ci': actual_ci_log.get(region, REGIONAL_CARBON_INTENSITY[region])
        }
    return carbon_by_region

def ssh_connect_with_retry_with_run(ssh, ip_address, retries, name, num):
    if retries > 3:
        return False
    privkey = paramiko.RSAKey.from_private_key_file('/home/rohan/rohan.pem')
    try:
        ssh.connect(hostname=ip_address, timeout=7200, username='ubuntu', pkey=privkey)
    except Exception:
        time.sleep(5)
        ssh_connect_with_retry_with_run(ssh, ip_address, retries + 1, name, num)
    stdin, stdout, stderr = ssh.exec_command(f"python3 /home/ubuntu/run_{name}.py {num}")
    stdout.channel.recv_exit_status()
    sftp_client = ssh.open_sftp()
    sftp_client.get(f'/home/ubuntu/{name}_output.txt', f'./{name}_output.txt')
    return 0

def run_ec2(name, num, is_optimization=False):
    global ec2_total_time, ec2_service_time, ec2_app_times, ec2_app_carbon, component_carbon_log
    cache_key = (name, num)
    if cache_key in ec2_cache and not is_optimization:
        runtime, carbon, service_time = ec2_cache[cache_key]
    else:
        ec2 = boto3.resource('ec2', region_name='us-east-2')
        instance = ec2.Instance(id=instance_ID)
        instance.wait_until_running()
        ip_address = list(ec2.instances.filter(InstanceIds=[instance_ID]))[0].public_ip_address
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        start_time = time.time()
        ssh_connect_with_retry_with_run(ssh, ip_address, 0, name, num)
        service_time = (time.time() - start_time) * factor
        runtime = service_time + S3_UPLOAD_TIME
        ssh.close()

        power_kw = EC2_POWER[ec2_name]
        energy_kwh = (power_kw * runtime) / 3600
        carbon = energy_kwh * CARBON_INTENSITY_US_EAST_2

        if not is_optimization:
            ec2_cache[cache_key] = (runtime, carbon, service_time)
            ec2_runtime_list.append(runtime)

    if not is_optimization:
        ec2_total_time += runtime
        ec2_service_time += service_time
        ec2_app_times[name] = ec2_app_times.get(name, 0) + runtime
        ec2_app_carbon[name] = ec2_app_carbon.get(name, 0) + carbon
        ec2_carbon_list.append((name, carbon))
        component_carbon_log.append((name, 'EC2', carbon))

    return runtime, carbon

def run_phase(phase_idx, ec2_counts, lam_regions, num_nodes, is_optimization=False):
    global ec2_total_time
    try:
        apps = app[phase_idx]
        if not isinstance(ec2_counts, list) or len(ec2_counts) != len(apps):
            raise ValueError(f"ec2_counts must be a list of length {len(apps)} for phase {phase_idx}")
        if not isinstance(lam_regions, dict):
            raise ValueError(f"lam_regions must be a dict for phase {phase_idx}")

        speculative_warmup(phase_idx)
        num_ec2_list = ec2_counts[:]
        num_lam_list = [num_of_app[phase_idx][i] - ec2_counts[i] for i in range(len(apps))]
        threads = []
        phase_time_start = time.time()

        for j, (app_name, num_ec2, num_lam) in enumerate(zip(apps, num_ec2_list, num_lam_list)):
            if num_ec2 > 0:
                t1 = threading.Thread(target=run_ec2, args=(app_name, num_ec2, is_optimization))
                t1.start()
                threads.append(t1)
            if num_lam > 0:
                region = lam_regions.get(app_name, 'us-east-1')
                predicted_ci = predict_ci(region, time.time())
                actual_ci = actual_ci_log.get(region, predicted_ci)
                if abs(actual_ci - predicted_ci) / predicted_ci > CI_THRESHOLD:
                    num_ec2_list[j] += num_lam
                    num_lam_list[j] = 0
                    t1 = threading.Thread(target=run_ec2, args=(app_name, num_lam, is_optimization))
                    t1.start()
                    threads.append(t1)
                    if not is_optimization:
                        ec2_total_time += predict_queue_wait_time(num_nodes)
                else:
                    t2 = threading.Thread(target=run_lam, args=(app_name, num_lam, region, False, is_optimization))
                    t2.start()
                    threads.append(t2)

        for t in threads:
            t.join()

        phase_duration = time.time() - phase_time_start
        if not is_optimization:
            phase_time.append(phase_duration)
        return phase_duration, 0
    except Exception:
        return 0, 0

def calc_cost():
    lambda_cost = sum(lam_cost_list) if lam_cost_list else 0
    ec2_cost = sum(ec2_runtime_list) * cost_of_ec2 if ec2_runtime_list else 0
    s3_time_cost = sum(phase_time) * s3_cost if phase_time else 0
    total_cost = lambda_cost + ec2_cost + s3_time_cost
    return total_cost

def calc_total_carbon_footprint():
    ec2_total = sum(carbon for _, carbon in ec2_carbon_list)
    lam_total = sum(carbon for _, carbon in lam_carbon_list)
    return ec2_total + lam_total, ec2_total, lam_total

def simulate_workflow(num_nodes, ec2_assignments, lam_assignments):
    global ec2_runtime_list, lam_cost_list, phase_time, ec2_carbon_list, lam_carbon_list, ec2_total_time, lam_total_time, ec2_service_time, lam_service_time
    global ec2_app_times, ec2_app_carbon, lam_app_times, lam_app_carbon, component_carbon_log, actual_ci_log
    ec2_runtime_list, lam_cost_list, phase_time, ec2_carbon_list, lam_carbon_list = [], [], [], [], []
    ec2_total_time, lam_total_time, ec2_service_time, lam_service_time = 0, 0, 0, 0
    ec2_app_times, ec2_app_carbon, lam_app_times, lam_app_carbon = {}, {}, {}, {}
    component_carbon_log, actual_ci_log = [], {}

    total_time = 0
    total_carbon = 0

    for phase_idx in range(phase):
        exe_time, _ = run_phase(phase_idx, ec2_assignments[phase_idx], lam_assignments[phase_idx], num_nodes, is_optimization=False)
        total_time += exe_time
        if sum(ec2_assignments[phase_idx]) > 0:
            queue_time = predict_queue_wait_time(num_nodes)
            total_time += queue_time
            ec2_total_time += queue_time

    total_cost = calc_cost()
    total_carbon_footprint, ec2_carbon, lam_carbon = calc_total_carbon_footprint()
    total_carbon += total_carbon_footprint
    total_service_time = ec2_service_time + lam_service_time
    return total_time, total_carbon, total_cost, ec2_total_time, lam_total_time, ec2_carbon, lam_carbon, ec2_service_time, lam_service_time, total_service_time

def optimize_phase(args):
    phase_idx, num_nodes = args
    apps = app[phase_idx]
    total_tasks = num_of_app[phase_idx]
    ec2_counts = [0] * len(apps)
    lam_regions = {}
    remaining_cost = MAX_COST
    total_time = 0
    total_carbon = 0

    LAMBDA_FACTOR = config['LAMBDA_FACTOR'] if config['LAMBDA_FACTOR'] is not None else 0.8
    EC2_THRESHOLD = config['EC2_THRESHOLD'] if config['EC2_THRESHOLD'] is not None else 0.8

    decisions = []
    for j, (app_name, num_tasks) in enumerate(zip(apps, total_tasks)):
        ec2_time, ec2_carbon = run_ec2(app_name, 1, is_optimization=True)
        ec2_queue_time = predict_queue_wait_time(num_nodes) if num_nodes > 0 else 0
        ec2_total_time_val = ec2_time + ec2_queue_time
        ec2_cost = ec2_time * cost_of_ec2
        ec2_score = ec2_total_time_val + ec2_carbon

        lam_time, lam_carbon, _ = run_lam(app_name, 1, 'us-east-1', False, is_optimization=True)
        lam_cost = lam_time * cost_of_lambda
        lam_score = (lam_time + lam_carbon) * (LAMBDA_FACTOR if LAMBDA_FACTOR is not None else 1.0)

        threshold = EC2_THRESHOLD if EC2_THRESHOLD is not None else 1.0
        if ec2_score < lam_score * threshold and ec2_cost * num_tasks <= remaining_cost and num_nodes > sum(ec2_counts):
            decisions.append((j, 'ec2', ec2_score, ec2_time, ec2_carbon, ec2_cost))
        else:
            decisions.append((j, 'lambda', lam_score, lam_time, lam_carbon, lam_cost))
            lam_regions[app_name] = 'us-east-1'

    for j, platform, score, unit_time, unit_carbon, unit_cost in sorted(decisions, key=lambda x: x[2]):
        app_name = apps[j]
        num_tasks = total_tasks[j]
        if platform == 'ec2' and num_nodes > sum(ec2_counts) and unit_cost * num_tasks <= remaining_cost:
            ec2_counts[j] = num_tasks
            total_time += unit_time * num_tasks + predict_queue_wait_time(num_nodes)
            total_carbon += unit_carbon * num_tasks
            remaining_cost -= unit_cost * num_tasks
        else:
            ec2_counts[j] = 0
            total_time += unit_time * num_tasks
            total_carbon += unit_carbon * num_tasks
            remaining_cost -= unit_cost
            lam_regions[app_name] = lam_regions.get(app_name, 'us-east-1')

    return phase_idx, ec2_counts, lam_regions, total_time, total_carbon, remaining_cost

def optimize_workflow():
    num_nodes_options = [5]
    best_time = float('inf')
    best_carbon = float('inf')
    best_config = None

    for num_nodes in num_nodes_options:
        with Pool(processes=cpu_count()) as pool:
            phase_results = pool.map(optimize_phase, [(i, num_nodes) for i in range(phase)])

        ec2_assignments = [None] * phase
        lam_assignments = [None] * phase
        total_time = 0
        total_carbon = 0
        total_cost = 0

        for phase_idx, ec2_counts, lam_regions, phase_time_val, phase_carbon, phase_cost in phase_results:
            ec2_assignments[phase_idx] = ec2_counts
            lam_assignments[phase_idx] = lam_regions
            total_time += phase_time_val
            total_carbon += phase_carbon
            total_cost += phase_cost

        sim_time, sim_carbon, sim_cost, ec2_time, lam_time, ec2_carbon, lam_carbon, ec2_serv, lam_serv, total_serv = simulate_workflow(num_nodes, ec2_assignments, lam_assignments)
        if sim_cost <= MAX_COST and (sim_time + sim_carbon) < (best_time + best_carbon):
            best_time = sim_time
            best_carbon = sim_carbon
            best_config = (num_nodes, ec2_assignments, lam_assignments, ec2_time, lam_time, ec2_carbon, lam_carbon, ec2_serv, lam_serv, total_serv)

    return best_config, best_time, best_carbon

def optimize_carbon_optimal():
    num_nodes_options = [5]
    best_carbon = float('inf')
    best_config = None
    sim_time = 0

    for num_nodes in num_nodes_options:
        ec2_assignments = [None] * phase
        lam_assignments = [None] * phase
        remaining_cost = MAX_COST

        for phase_idx in range(phase):
            apps = app[phase_idx]
            total_tasks = num_of_app[phase_idx]
            ec2_counts = [0] * len(apps)
            lam_regions = {}

            decisions = []
            for j, (app_name, num_tasks) in enumerate(zip(apps, total_tasks)):
                ec2_time, ec2_carbon = run_ec2(app_name, 1, is_optimization=True)
                ec2_cost = ec2_time * cost_of_ec2

                lam_time, lam_carbon, _ = run_lam(app_name, 1, 'us-east-1', False, is_optimization=True)
                lam_cost = lam_time * cost_of_lambda

                if ec2_carbon < lam_carbon and ec2_cost * num_tasks <= remaining_cost and num_nodes > sum(ec2_counts):
                    decisions.append((j, 'ec2', ec2_carbon, ec2_time, ec2_carbon, ec2_cost))
                else:
                    decisions.append((j, 'lambda', lam_carbon, lam_time, lam_carbon, lam_cost))
                    lam_regions[app_name] = 'us-east-1'

            for j, platform, _, unit_time, unit_carbon, unit_cost in sorted(decisions, key=lambda x: x[2]):
                app_name = apps[j]
                num_tasks = total_tasks[j]
                if platform == 'ec2' and num_nodes > sum(ec2_counts) and unit_cost * num_tasks <= remaining_cost:
                    ec2_counts[j] = num_tasks
                    remaining_cost -= unit_cost * num_tasks
                else:
                    ec2_counts[j] = 0
                    remaining_cost -= unit_cost
                    lam_regions[app_name] = lam_regions.get(app_name, 'us-east-1')

            ec2_assignments[phase_idx] = ec2_counts
            lam_assignments[phase_idx] = lam_regions

        sim_time, sim_carbon, sim_cost, ec2_time, lam_time, ec2_carbon, lam_carbon, ec2_serv, lam_serv, total_serv = simulate_workflow(num_nodes, ec2_assignments, lam_assignments)
        if sim_cost <= MAX_COST and sim_carbon < best_carbon:
            best_carbon = sim_carbon
            best_config = (num_nodes, ec2_assignments, lam_assignments, ec2_time, lam_time, ec2_carbon, lam_carbon, ec2_serv, lam_serv, total_serv)

    return best_config, sim_time, best_carbon

def optimize_service_time_optimal():
    num_nodes_options = [5]
    best_time = float('inf')
    best_config = None
    sim_carbon = 0

    for num_nodes in num_nodes_options:
        ec2_assignments = [None] * phase
        lam_assignments = [None] * phase
        remaining_cost = MAX_COST

        for phase_idx in range(phase):
            apps = app[phase_idx]
            total_tasks = num_of_app[phase_idx]
            ec2_counts = [0] * len(apps)
            lam_regions = {}

            decisions = []
            for j, (app_name, num_tasks) in enumerate(zip(apps, total_tasks)):
                ec2_time, ec2_carbon = run_ec2(app_name, 1, is_optimization=True)
                ec2_queue_time = predict_queue_wait_time(num_nodes)
                ec2_total_time_val = ec2_time + ec2_queue_time
                ec2_cost = ec2_time * cost_of_ec2

                lam_time, lam_carbon, _ = run_lam(app_name, 1, 'us-east-1', False, is_optimization=True)
                lam_cost = lam_time * cost_of_lambda

                if ec2_total_time_val < lam_time and ec2_cost * num_tasks <= remaining_cost and num_nodes > sum(ec2_counts):
                    decisions.append((j, 'ec2', ec2_total_time_val, ec2_time, ec2_carbon, ec2_cost))
                else:
                    decisions.append((j, 'lambda', lam_time, lam_time, lam_carbon, lam_cost))
                    lam_regions[app_name] = 'us-east-1'

            for j, platform, _, unit_time, unit_carbon, unit_cost in sorted(decisions, key=lambda x: x[2]):
                app_name = apps[j]
                num_tasks = total_tasks[j]
                if platform == 'ec2' and num_nodes > sum(ec2_counts) and unit_cost * num_tasks <= remaining_cost:
                    ec2_counts[j] = num_tasks
                    remaining_cost -= unit_cost * num_tasks
                else:
                    ec2_counts[j] = 0
                    remaining_cost -= unit_cost
                    lam_regions[app_name] = 'us-east-1'

            ec2_assignments[phase_idx] = ec2_counts
            lam_assignments[phase_idx] = lam_regions

        sim_time, sim_carbon, sim_cost, ec2_time, lam_time, ec2_carbon, lam_carbon, ec2_serv, lam_serv, total_serv = simulate_workflow(num_nodes, ec2_assignments, lam_assignments)
        if sim_cost <= MAX_COST and sim_time < best_time:
            best_time = sim_time
            best_config = (num_nodes, ec2_assignments, lam_assignments, ec2_time, lam_time, ec2_carbon, lam_carbon, ec2_serv, lam_serv, total_serv)

    return best_config, best_time, sim_carbon

def run_mashup():
    num_nodes_options = [5]
    num_nodes = random.choice(num_nodes_options)
    ec2_assignments = [None] * phase
    lam_assignments = [None] * phase

    for phase_idx in range(phase):
        apps = app[phase_idx]
        total_tasks = num_of_app[phase_idx]
        ec2_counts = []
        lam_regions = {}

        for app_name, num_tasks in zip(apps, total_tasks):
            max_ec2_tasks = min(num_tasks, num_nodes - sum(ec2_counts)) if num_nodes > sum(ec2_counts) else 0
            num_ec2 = random.randint(0, max_ec2_tasks) if max_ec2_tasks > 0 else 0
            ec2_counts.append(num_ec2)
            if num_tasks - num_ec2 > 0:
                lam_regions[app_name] = 'us-east-1'

        ec2_assignments[phase_idx] = ec2_counts
        lam_assignments[phase_idx] = lam_regions

    sim_time, sim_carbon, sim_cost, ec2_time, lam_time, ec2_carbon, lam_carbon, ec2_serv, lam_serv, total_serv = simulate_workflow(num_nodes, ec2_assignments, lam_assignments)
    return (num_nodes, ec2_assignments, lam_assignments, ec2_time, lam_time, ec2_carbon, lam_carbon, ec2_serv, lam_serv, total_serv), sim_time, sim_carbon, sim_cost

def run_mashup_service_based():
    num_nodes_options = [5]
    num_nodes = random.choice(num_nodes_options)
    ec2_assignments = [None] * phase
    lam_assignments = [None] * phase

    for phase_idx in range(phase):
        apps = app[phase_idx]
        total_tasks = num_of_app[phase_idx]
        ec2_counts = []
        lam_regions = {}

        for app_name, num_tasks in zip(apps, total_tasks):
            start_time = time.time()
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ec2 = boto3.resource('ec2', region_name='us-east-2')
            ip_address = list(ec2.instances.filter(InstanceIds=[instance_ID]))[0].public_ip_address
            ssh_connect_with_retry_with_run(ssh, ip_address, 0, app_name, 1)
            ec2_svc_time = time.time() - start_time
            ssh.close()

            _, _, lam_svc_time = run_lam(app_name, 1, 'us-east-1', False, True)

            max_ec2_tasks = min(num_tasks, num_nodes - sum(ec2_counts)) if num_nodes > sum(ec2_counts) else 0
            if ec2_svc_time < lam_svc_time and max_ec2_tasks > 0:
                num_ec2 = min(num_tasks, max_ec2_tasks)
                num_lam = num_tasks - num_ec2
            else:
                num_ec2 = 0
                num_lam = num_tasks

            ec2_counts.append(num_ec2)
            if num_lam > 0:
                lam_regions[app_name] = 'us-east-1'

        ec2_assignments[phase_idx] = ec2_counts
        lam_assignments[phase_idx] = lam_regions

    sim_time, sim_carbon, sim_cost, ec2_time, lam_time, ec2_carbon, lam_carbon, ec2_serv, lam_serv, total_serv = simulate_workflow(num_nodes, ec2_assignments, lam_assignments)
    return (num_nodes, ec2_assignments, lam_assignments, ec2_time, lam_time, ec2_carbon, lam_carbon, ec2_serv, lam_serv, total_serv), sim_time, sim_carbon, sim_cost

def reset_globals():
    global ec2_runtime_list, lam_cost_list, phase_time, ec2_carbon_list, lam_carbon_list
    global ec2_total_time, lam_total_time, ec2_service_time, lam_service_time
    global ec2_app_times, ec2_app_carbon, lam_app_times, lam_app_carbon, component_carbon_log
    global lam_cache, ec2_cache
    ec2_runtime_list, lam_cost_list, phase_time, ec2_carbon_list, lam_carbon_list = [], [], [], [], []
    ec2_total_time, lam_total_time, ec2_service_time, lam_service_time = 0, 0, 0, 0
    ec2_app_times, ec2_app_carbon, lam_app_times, lam_app_carbon = {}, {}, {}, {}
    component_carbon_log = []
    lam_cache.clear()
    ec2_cache.clear()


if __name__ == "__main__":
    # ---- SRAsearch workflow configuration ----
    app_name = "SRAsearch"
    phase = 4
    app = [
        ['fasterq_dump', 'bowtie2_build'],
        ['bowtie2'],
        ['merge1'],
        ['merge2']
    ]
    num_of_app = [[50, 1], [50], [2], [1]]
    num_in_ec2 = [[0,  1], [0],  [2], [1]]

    instance_ID    = 'i-06e66cc90ec546df3'
    ec2_name       = "m5a.8xlarge"
    cost_of_lambda = 0.00005        # per-sec cost
    s3_cost        = 8.75190259e-9  # cost per sec/GB up to 50 TB

    ec2_cost_list = [0.086, 0.172, 0.344, 0.688, 1.376, 2.064, 2.752, 4.128]
    ec2_name_list = ['m5a.large', 'm5a.xlarge', 'm5a.2xlarge', 'm5a.4xlarge',
                     'm5a.8xlarge', 'm5a.12xlarge', 'm5a.16xlarge', 'm5a.24xlarge']
    cost_of_ec2 = ec2_cost_list[ec2_name_list.index(ec2_name)] / 3600

    num_in_lam = [
        [num_of_app[i][j] - num_in_ec2[i][j] for j in range(len(num_of_app[i]))]
        for i in range(len(num_of_app))
    ]

    # ---- Our Scheme (Default) ----
    reset_globals()
    our_config, our_time, our_carbon = optimize_workflow()
    num_nodes, ec2_assignments, lam_assignments, _, _, _, _, our_ec2_service, our_lam_service, our_total_service = our_config
    phase_count = 0
    while phase_count < phase:
        run_phase(phase_count, ec2_assignments[phase_count], lam_assignments[phase_count], num_nodes, is_optimization=False)
        phase_count += 1
    our_cost = calc_cost()
    our_total_carbon, our_ec2_carbon, our_lam_carbon = calc_total_carbon_footprint()

    # ---- Simulate Lambda carbon across regions ----
    lambda_carbon_sim = {}
    for phase_idx, apps in enumerate(app):
        for a in apps:
            num_tasks = num_of_app[phase_idx][apps.index(a)]
            lambda_carbon_sim[a] = simulate_lambda_carbon(a, min(num_tasks, 10))

    # ---- Our Scheme (Custom) ----
    set_config(lambda_factor=0.3, ec2_threshold=0.2)
    reset_globals()
    our_config_custom, our_time_custom, our_carbon_custom = optimize_workflow()
    num_nodes_custom, ec2_assignments_custom, lam_assignments_custom, _, _, _, _, our_ec2_service_custom, our_lam_service_custom, our_total_service_custom = our_config_custom
    phase_count = 0
    while phase_count < phase:
        run_phase(phase_count, ec2_assignments_custom[phase_count], lam_assignments_custom[phase_count], num_nodes_custom, is_optimization=False)
        phase_count += 1
    our_cost_custom = calc_cost()
    our_total_carbon_custom, our_ec2_carbon_custom, our_lam_carbon_custom = calc_total_carbon_footprint()

    # ---- Our Scheme (No Bias) ----
    unset_config('LAMBDA_FACTOR')
    reset_globals()
    our_config_no_bias, our_time_no_bias, our_carbon_no_bias = optimize_workflow()
    num_nodes_no_bias, ec2_assignments_no_bias, lam_assignments_no_bias, _, _, _, _, our_ec2_service_no_bias, our_lam_service_no_bias, our_total_service_no_bias = our_config_no_bias
    phase_count = 0
    while phase_count < phase:
        run_phase(phase_count, ec2_assignments_no_bias[phase_count], lam_assignments_no_bias[phase_count], num_nodes_no_bias, is_optimization=False)
        phase_count += 1
    our_cost_no_bias = calc_cost()
    our_total_carbon_no_bias, our_ec2_carbon_no_bias, our_lam_carbon_no_bias = calc_total_carbon_footprint()

    # ---- Carbon-Optimal Baseline ----
    reset_globals()
    carbon_config, carbon_time, carbon_carbon = optimize_carbon_optimal()
    carbon_num_nodes, carbon_ec2_assignments, carbon_lam_assignments, _, _, _, _, carbon_ec2_service, carbon_lam_service, carbon_total_service = carbon_config
    phase_count = 0
    while phase_count < phase:
        run_phase(phase_count, carbon_ec2_assignments[phase_count], carbon_lam_assignments[phase_count], carbon_num_nodes, is_optimization=False)
        phase_count += 1
    carbon_cost = calc_cost()
    carbon_total_carbon, carbon_ec2_carbon, carbon_lam_carbon = calc_total_carbon_footprint()

    # ---- Service Time-Optimal Baseline ----
    reset_globals()
    time_config, time_time, time_carbon = optimize_service_time_optimal()
    time_num_nodes, time_ec2_assignments, time_lam_assignments, _, _, _, _, time_ec2_service, time_lam_service, time_total_service = time_config
    phase_count = 0
    while phase_count < phase:
        run_phase(phase_count, time_ec2_assignments[phase_count], time_lam_assignments[phase_count], time_num_nodes, is_optimization=False)
        phase_count += 1
    time_cost = calc_cost()
    time_total_carbon, time_ec2_carbon, time_lam_carbon = calc_total_carbon_footprint()

    # ---- MASHUP Baseline (Random) ----
    reset_globals()
    mashup_config, mashup_time, mashup_carbon, mashup_cost = run_mashup()
    mashup_num_nodes, mashup_ec2_assignments, mashup_lam_assignments, mashup_ec2_time, mashup_lam_time, mashup_ec2_carbon, mashup_lam_carbon, mashup_ec2_service, mashup_lam_service, mashup_total_service = mashup_config

    # ---- MASHUP Baseline (Service-Based) ----
    reset_globals()
    mashup_serv_config, mashup_serv_time, mashup_serv_carbon, mashup_serv_cost = run_mashup_service_based()
    mashup_serv_num_nodes, mashup_serv_ec2_assignments, mashup_serv_lam_assignments, mashup_serv_ec2_time, mashup_serv_lam_time, mashup_serv_ec2_carbon, mashup_serv_lam_carbon, mashup_serv_ec2_service, mashup_serv_lam_service, mashup_serv_total_service = mashup_serv_config
