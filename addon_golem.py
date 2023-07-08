import sys
import site
import os
import zipfile
import subprocess
import time
import logging
import signal
import ensurepip
import json
import asyncio
import tempfile
import importlib
from pathlib import Path
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
from multiprocessing import Process, Value, Queue
from decimal import Decimal

def init_payment(network):
    cmd = ["yagna", "payment", "init", "--sender", "--network=" + network, "--driver=erc20"]
    subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def get_appkey():
    cmd = ["yagna", "app-key", "list", "--json"]
    json_key_list = ''
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:
        for line in proc.stdout:
            json_key_list += line.strip()
    return json.loads(json_key_list)[0]['key']

async def main( queue=None,
                payment_driver=None,
                payment_network=None,
                subnet_tag=None,
                budget=None,
                interval_payment=0,
                start_price=0,
                cpu_price=0,
                env_price=0,
                timeout_global=0,
                timeout_upload=0,
                timeout_render=0,
                workers=0,
                memory=0,
                storage=0,
                threads=0,
                format=None,
                scene=None,
                frames=None,
                output_dir=None,
                project_directory=None):

    from yapapi import Golem, Task, WorkContext
    from yapapi.payload import vm
    from yapapi.rest.activity import BatchTimeoutError
    from yapapi.events import AgreementConfirmed, TaskAccepted, ActivityCreateFailed, TaskRejected, WorkerFinished, TaskRejected
    from yapapi import events
    from yapapi.strategy import LeastExpensiveLinearPayuMS
    from yapapi.contrib.strategy import ProviderFilter
    from yapapi.props import com

    bad_providers = set()

    if format in ["OPEN_EXR_MULTILAYER", "OPEN_EXR"]:
        ext = "exr"
    else:
        ext = format.lower()

    package = await vm.repo(
        image_hash = "b5e19a68e0268c0e72309048b5e6a29512e3ecbabd355c6ac590f75d",
        min_mem_gib = memory,
        min_storage_gib = storage,
        min_cpu_threads = threads,
        capabilities = ["cuda"],
    )

    input_file = output_dir + "/archive.zip"
    with zipfile.ZipFile(input_file, 'w') as f:
        for subdir, dirs, files in os.walk(project_directory):
                for file in files:
                    srcpath = os.path.join(subdir, file)
                    dstpath_in_zip = os.path.relpath(srcpath, start=project_directory)
                    with open(srcpath, 'rb') as infile:
                        f.writestr(dstpath_in_zip, infile.read())

    def event_consumer(event: events.Event):
        if isinstance(event, events.AgreementConfirmed):
            print('AgreementConfirmed ' + event.provider_id)
            queue.put('add_provider')
        elif isinstance(event, (events.ActivityCreateFailed, events.TaskRejected, events.WorkerFinished, events.TaskRejected)):
            bad_providers.add(event.provider_id)
            queue.put('remove_provider')
        elif isinstance(event, events.TaskAccepted):
            print('Task data ' + str(event.task.data) + ' accepted from provider ' + event.agreement.details.provider_node_info.name)
            queue.put('frame_finished')

    async def worker(ctx: WorkContext, tasks):
        script = ctx.new_script(timeout=timedelta(minutes=(timeout_upload + timeout_render)))
        script.upload_file(input_file, "/golem/resources/archive.zip");

        try:
            script.run("/bin/sh", "-c", "(rm -rf /golem/output/*) || true")
            script.run("/bin/sh", "-c", "unzip -o /golem/resources/archive.zip -d /golem/resources/")
            cmd_display = "PCIID=$(nvidia-xconfig --query-gpu-info | grep 'PCI BusID' | awk -F'PCI BusID : ' '{print $2}') && (nvidia-xconfig --busid=$PCIID --use-display-device=none --virtual=1280x1024 || true) && ((Xorg :1 &) || true) && sleep 5"
            script.run("/bin/sh", "-c", cmd_display)

            async for task in tasks:
                frame = task.data
                cmd_render = "(DISPLAY=:1 blender -b /golem/resources/" + scene + ".blend -o /golem/output/ -noaudio -F " + format + " -f " + str(frame) + " -- --cycles-device CUDA) || true"
                script.run("/bin/sh", "-c", cmd_render)
                output_file = f"{output_dir}/{frame:04d}.{ext}"
                future_result = script.download_file(f"/golem/output/{frame:04d}.{ext}", output_file)

                yield script
                result = await future_result

                if result.success:
                    task.accept_result(result=f"{frame:04d}")
                else:
                    task.reject_result(reason="bad result", retry=True)

                script = ctx.new_script(timeout=timedelta(minutes=timeout_render))

        except BatchTimeoutError:
            bad_providers.add(ctx.provider_id)
            queue.put('remove_provider')
            raise

    golem = Golem(
        budget=budget,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    )

    golem.strategy = ProviderFilter(LeastExpensiveLinearPayuMS(
        max_fixed_price=Decimal(str(start_price)),
        max_price_for={
            com.Counter.CPU: Decimal(str(cpu_price)),
            com.Counter.TIME: Decimal(str(env_price))
        }
    ), lambda provider_id: provider_id not in bad_providers)

    async with golem:
        golem.add_event_consumer(event_consumer)

        completed_tasks = golem.execute_tasks(
            worker,
            [Task(data=frame) for frame in frames],
            payload=package,
            max_workers=workers,
            timeout=timedelta(hours=timeout_global)
        )

        async for task in completed_tasks:
            frames.remove(int(task.result))

def render(main_blend_file, project_directory, output_directory, frames, queue, network, budget, start_price, cpu_price, env_price, timeout_global, timeout_upload, timeout_render, workers, memory, storage, threads, format):      

    importlib.reload(site)
    from yapapi.log import enable_default_logger

    init_payment(network)
    app_key = get_appkey()
    os.environ['YAGNA_APPKEY'] = app_key

    enable_default_logger(
        log_file=output_directory + '/requestor.log',
        debug_activity_api=True,
        debug_market_api=True,
        debug_payment_api=True,
        debug_net_api=True,
    )

    loop = asyncio.get_event_loop()
    task = loop.create_task(main(
            queue = queue,
            payment_driver = "erc20",
            payment_network = network,
            subnet_tag = "public",
            budget = budget,
            interval_payment = 0,
            start_price = start_price,
            cpu_price = cpu_price,
            env_price = env_price,
            timeout_global = timeout_global,
            timeout_upload = timeout_upload,
            timeout_render = timeout_render,
            workers = workers,
            memory = memory,
            storage = storage,
            threads = threads,
            format = format,
            scene = main_blend_file,
            frames = frames,
            output_dir = output_directory,
            project_directory = project_directory
        ))
    loop.run_until_complete(task)

if __name__ == "__main__":
    register()