bl_info = {
    "name": "Golem Cycles GPU Rendering",
    "author": "Norbert Mauger",
    "version": (0, 1),
    "blender": (3, 0, 0),
    "location": "Properties > Render",
    "description": "Decentralized Cycles GPU Rendering",
    "category": "Render",
}

import bpy
from bpy.app.handlers import persistent
import sys
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

script_directory = bpy.utils.user_resource('SCRIPTS')
sys.path.append(f"{script_directory}/addons/Golem_Cycles_GPU_Rendering")

from addon_golem import render

#################################################################################################################################

def ShowMessageBox(message = "", title = "Message Box", icon = 'INFO'):
    def draw(self, context):
        self.layout.label(text=message)
    bpy.context.window_manager.popup_menu(draw, title = title, icon = icon)

def create_output_directory(main_output_directory):
    project_name = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    output_dir = main_output_directory + '/' + project_name
    os.mkdir(output_dir)
    return output_dir

count = 0
render_process = None
queue = 0
frames = None

@persistent
def init(scene):
	bpy.context.scene.golem_settings.start_frame = bpy.context.scene.frame_start
	bpy.context.scene.golem_settings.end_frame = bpy.context.scene.frame_end
	bpy.context.scene.golem_settings.step_frame = bpy.context.scene.render.fps

class Golem_Render(bpy.types.Operator):
    bl_idname = "golem_render.render"
    bl_label = "Render"

    def execute(self, context):
        global render_process
        global count
        global queue
        global frames
        global render_btn
        global cancel_btn
        global progress_btn
        global providers_btn
        global running

        if not running:
            main_blend_file = bpy.path.display_name_from_filepath(bpy.data.filepath)
            main_output_directory = bpy.path.abspath(bpy.context.scene.render.filepath)
            output_directory = create_output_directory(main_output_directory)
            children = Path(bpy.path.abspath(bpy.data.filepath))
            root = Path(main_output_directory)

            if root in children.parents:
                ShowMessageBox("Please select folder outside project directory", "Output folder error", 'ERROR')
                return {'FINISHED'}

            render_btn = False
            progress_btn = True
            providers_btn = True
            cancel_btn = True

            count = 0
            bpy.context.scene.golem_settings.progress = 0
            bpy.context.scene.golem_settings.providers = 0

            bpy.app.timers.register(update_progress)

            frames = list(range(bpy.context.scene.golem_settings.start_frame, bpy.context.scene.golem_settings.end_frame+1, bpy.context.scene.golem_settings.step_frame))
            queue = Queue()

            render_process = Process(target=render, args=(main_blend_file,
                                                            os.path.dirname(bpy.data.filepath),
                                                            output_directory,
                                                            frames,
                                                            queue,
                                                            bpy.context.scene.golem_settings.network,
                                                            bpy.context.scene.golem_settings.budget,
                                                            (bpy.context.scene.golem_settings.start_price/3600000),
                                                            (bpy.context.scene.golem_settings.cpu_price/3600000),
                                                            (bpy.context.scene.golem_settings.env_price/3600000),
                                                            bpy.context.scene.golem_settings.timeout_global,
                                                            bpy.context.scene.golem_settings.timeout_upload,
                                                            bpy.context.scene.golem_settings.timeout_render,
                                                            bpy.context.scene.golem_settings.workers,
                                                            bpy.context.scene.golem_settings.memory,
                                                            bpy.context.scene.golem_settings.storage,
                                                            bpy.context.scene.golem_settings.threads,
                                                            bpy.context.scene.golem_settings.output_format))
            running = True
            render_process.start()

        return {'FINISHED'}

def update_progress():
    global count
    global queue
    global render_process
    global render_btn
    global progress_btn
    global providers_btn
    global cancel_btn
    global running

    try:
        msg = queue.get(block=False)
        if msg == "frame_finished":
            if frames is not None:
                count += 1
                bpy.context.scene.golem_settings.progress = int(count*100/len(frames))
                if count == len(frames):
                    bpy.app.timers.unregister(update_progress)
                    render_btn = True
                    progress_btn = False
                    providers_btn = False
                    cancel_btn = False
                    running = False
        elif msg == "add_provider":
            bpy.context.scene.golem_settings.providers += 1
        elif msg == "remove_provider":
            bpy.context.scene.golem_settings.providers -= 1
    except:
        pass

    return 1.0

class Golem_Cancel(bpy.types.Operator):
    bl_idname = "golem_cancel.cancel"
    bl_label = "Cancel"

    def execute(self, context):
        global render_process
        global render_btn
        global progress_btn
        global providers_btn
        global cancel_btn
        global running

        if running:
            running = False
            render_process.terminate()
            bpy.app.timers.unregister(update_progress)
            render_btn = True
            progress_btn = False
            providers_btn = False
            cancel_btn = False
            print("render_process terminated")
        return {'FINISHED'}

##########################################################################################################

def set_start_frame(self, value):
	if value <= self["end_frame"]:
		if value >= bpy.context.scene.frame_start:
			self["start_frame"] = value
		else:
			self["start_frame"] = bpy.context.scene.frame_start

def get_start_frame(self):
    if "start_frame" not in self.keys():
        self["start_frame"] = bpy.context.scene.frame_start
    return self["start_frame"]

def set_end_frame(self, value):
	if value >= self["start_frame"]:
		if value <= bpy.context.scene.frame_end:
			self["end_frame"] = value
		else:
			self["end_frame"] = bpy.context.scene.frame_end

def get_end_frame(self):
    if "end_frame" not in self.keys():
        self["end_frame"] = bpy.context.scene.frame_end
    return self["end_frame"]

def update_ui_progress(self, context):
    return None

def update_ui_providers(self, context):
    return None

class GolemRenderSettings(bpy.types.PropertyGroup):
    progress: bpy.props.IntProperty(name="Progress (%)", default=0, min=0, max=100, update=update_ui_progress)
    providers: bpy.props.IntProperty(name="Providers", default=0, min=0, max=100, update=update_ui_providers)
    workers: bpy.props.IntProperty(name="Workers", default=1, min=1, max=128)
    memory: bpy.props.IntProperty(name="Memory (GB)", default=8, min=1, max=1024)
    storage: bpy.props.IntProperty(name="Storage (GB)", default=8, min=1, max=1024)
    threads: bpy.props.IntProperty(name="Threads", default=8, min=1, max=128)
    start_frame: bpy.props.IntProperty(name="Start", min=1, set=set_start_frame, get=get_start_frame)
    end_frame: bpy.props.IntProperty(name="End", min=1, set=set_end_frame, get=get_end_frame)
    step_frame: bpy.props.IntProperty(name="Step", default=1, min=1, max=100)
    budget: bpy.props.IntProperty(name="Budget (GLM)", default=10, min=1, max=100)
    start_price: bpy.props.IntProperty(name="start", default=0, min=0, max=1000)
    cpu_price: bpy.props.IntProperty(name="cpu/h", default=0, min=0, max=1000)
    env_price: bpy.props.IntProperty(name="env/h", default=0, min=0, max=1000)
    timeout_global: bpy.props.IntProperty(name="Global (h)", default=4, min=1, max=24)
    timeout_upload: bpy.props.IntProperty(name="Upload (mn)", default=10, min=1, max=59)
    timeout_render: bpy.props.IntProperty(name="Render (mn)", default=10, min=1, max=59)
    output_format: bpy.props.EnumProperty(name="Output Format",
            items=(
                ("PNG", "PNG", ""),
                ("BMP", "BMP", ""),
                ("JPEG", "JPEG", ""),
                ("OPEN_EXR", "OPEN_EXR", ""),
                ("OPEN_EXR_MULTILAYER", "OPEN_EXR_MULTILAYER", "")
            ),
            default="PNG"
        )
    network: bpy.props.EnumProperty(name="Network",
            items=(
                ("goerli", "Goerli (ETH)", ""),
                ("mumbai", "Mumbai (MATIC)")
                ("polygon", "Polygon (MATIC)", "")
            ),
            default="mumbai"
        )

##########################################################################################################

running = False
render_btn = True
cancel_btn = False
progress_btn = False
providers_btn = False

class LayoutDemoPanel(bpy.types.Panel):
    bl_label = "Golem Cycles GPU Rendering"
    bl_idname = "RENDER_PT_golem"
    bl_space_type = 'PROPERTIES'
    bl_region_type = 'WINDOW'
    bl_context = "render"

    def draw(self, context):
        global render_btn
        global cancel_btn
        global progress_btn

        layout = self.layout
        scene = context.scene

        box_anim_settings = layout.box()

        row = box_anim_settings.row()
        row.prop(bpy.context.scene.golem_settings, "output_format")
        row = box_anim_settings.row()
        row.prop(scene.render, "filepath")

        row = box_anim_settings.row()
        row.prop(bpy.context.scene.golem_settings, "start_frame")
        row.prop(bpy.context.scene.golem_settings, "end_frame")
        row.prop(bpy.context.scene.golem_settings, "step_frame")

        box_golem_settings = layout.box()

        row = box_golem_settings.row()
        row.prop(bpy.context.scene.golem_settings, "network")

        row = box_golem_settings.row()
        row.prop(bpy.context.scene.golem_settings, "budget")

        row = box_golem_settings.row()
        row.prop(bpy.context.scene.golem_settings, "workers")
        row.prop(bpy.context.scene.golem_settings, "memory")
        row.prop(bpy.context.scene.golem_settings, "storage")
        row.prop(bpy.context.scene.golem_settings, "threads")

        row = box_golem_settings.row()
        row.label(text="Prices (mGLM):")
        row.prop(bpy.context.scene.golem_settings, "start_price")
        row.prop(bpy.context.scene.golem_settings, "cpu_price")
        row.prop(bpy.context.scene.golem_settings, "env_price")

        row = box_golem_settings.row()
        row.label(text="Timeouts:")
        row.prop(bpy.context.scene.golem_settings, "timeout_global")
        row.prop(bpy.context.scene.golem_settings, "timeout_upload")
        row.prop(bpy.context.scene.golem_settings, "timeout_render")

        row = layout.row()

        split = row.split(factor=0.5)
        coll = split.column()
        row = coll.row()
        row.operator("Golem_Render.render")
        row.enabled = render_btn
        row = coll.row()
        row.operator("Golem_Cancel.cancel")
        row.enabled = cancel_btn
        colr = split.column()
        row = colr.row()
        row.prop(bpy.context.scene.golem_settings, "progress")
        row.enabled = progress_btn
        row = colr.row()
        row.prop(bpy.context.scene.golem_settings, "providers")
        row.enabled = providers_btn

def register():
    try:
        import yapapi
    except:
        try:
            subprocess.run([sys.executable, "-m", "pip", "--version"], check=True)
        except subprocess.CalledProcessError:
            ensurepip.bootstrap()
        subprocess.run([sys.executable, "-m", "pip", "install", "yapapi"], check=True)

    bpy.utils.register_class(GolemRenderSettings)
    bpy.utils.register_class(Golem_Render)
    bpy.utils.register_class(Golem_Cancel)
    bpy.utils.register_class(LayoutDemoPanel)
    bpy.types.Scene.golem_settings = bpy.props.PointerProperty(type=GolemRenderSettings)

    bpy.app.handlers.load_post.append(init)

def unregister():
    bpy.utils.unregister_class(LayoutDemoPanel)
    bpy.utils.unregister_class(Golem_Render)
    bpy.utils.unregister_class(Golem_Cancel)
    bpy.utils.unregister_class(GolemRenderSettings)
    del bpy.types.Scene.golem_settings

#######################################################################################################################

if __name__ == "__main__":
    register()
