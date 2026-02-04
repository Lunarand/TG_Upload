import os
import logging
import asyncio
import time
import math
import yt_dlp
import glob
import io
import requests
import subprocess
import random
import re
import shutil
from telethon import TelegramClient, events, Button, errors

# --- IMPORT INSTALOADER (For Instagram Fix) ---
try:
    import instaloader
except ImportError:
    logging.error("Instaloader not found. Please add 'instaloader' to requirements.txt")

# --- CONFIGURATION ---
API_ID = int(os.getenv('API_ID')) 
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('TELEGRAM_TOKEN')
SECRET_KEY = os.getenv('KEY')

# --- LOGGING ---
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
# Silence Instaloader logs
logging.getLogger("instaloader").setLevel(logging.WARNING)

# --- STATE ---
AUTHORIZED_USERS = set()
USER_DATA = {} 
TASK_CANCEL = {}

# --- INITIALIZE CLIENT ---
client = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# --- HELPERS ---
def format_size(size_bytes):
    if size_bytes == 0: return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

def get_progress_bar(current, total, start_time):
    if total == 0: return ""
    percent = int(current / total * 100)
    bar_len = 10
    filled = int(percent / 100 * bar_len)
    bar = '■' * filled + '□' * (bar_len - filled)
    
    elapsed = time.time() - start_time
    speed_str = "0B/s"
    if elapsed > 0:
        speed = current / elapsed
        speed_str = f"{format_size(speed)}/s"
        
    return f"[{bar}] {percent}%\n📊 {format_size(current)} / {format_size(total)}\n🚀 {speed_str}"

def is_auth(user_id):
    return user_id in AUTHORIZED_USERS

# --- CRASH PREVENTION HELPER ---
async def safe_edit(message, text, buttons=None):
    try:
        await message.edit(text, buttons=buttons)
        return message
    except (errors.MessageIdInvalidError, errors.MessageNotModifiedError):
        try:
            return await message.respond(text, buttons=buttons)
        except:
            return message
    except Exception as e:
        # logging.error(f"Safe Edit Error: {e}") 
        return message

# --- QUEUE MANAGEMENT (FIXED) ---
async def queue_worker(event, user_id):
    """
    Robust queue worker that prevents getting stuck.
    """
    user_data = USER_DATA.get(user_id, {})
    if user_data.get('is_busy', False):
        return

    USER_DATA[user_id]['is_busy'] = True

    try:
        while True:
            queue = USER_DATA[user_id].get('queue', [])
            if not queue:
                break

            # Get next task
            task = queue.pop(0)
            
            # Run download
            try:
                await run_download(event, task['url'], task['quality'])
            except Exception as e:
                logging.error(f"Queue Task Error: {e}")
            
            # Small delay
            await asyncio.sleep(1)
            
    finally:
        # This guarantees the bot never gets stuck saying "Position 1" forever
        USER_DATA[user_id]['is_busy'] = False
        await event.respond("✅ **All tasks in queue finished.**")

# --- VIDEO PROCESSING ENGINES ---

async def ensure_faststart(filepath, status_msg):
    if not os.path.exists(filepath): return filepath
    if not filepath.lower().endswith(('.mp4', '.mov')): return filepath

    await safe_edit(status_msg, f"🚀 **Optimizing for Streaming...**\n(Fixing Glitches)")
    
    base, ext = os.path.splitext(filepath)
    output_path = f"{base}_stream{ext}"
    
    cmd = ['ffmpeg', '-y', '-i', filepath, '-c', 'copy', '-movflags', '+faststart', output_path]
    
    process = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    await process.communicate()
    
    if os.path.exists(output_path):
        os.remove(filepath)
        return output_path
        
    return filepath

async def transcode_video(filepath, target_height, status_msg):
    if not os.path.exists(filepath) or not target_height: return filepath
    
    try:
        cmd_probe = ['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=height', '-of', 'csv=p=0', filepath]
        res = subprocess.run(cmd_probe, capture_output=True, text=True)
        current_height = int(res.stdout.strip())
    except: return filepath 

    if current_height <= target_height: 
        return filepath

    status_msg = await safe_edit(status_msg, f"⚙️ **Processing...**\nConverting {current_height}p ➡️ {target_height}p")
    
    base, ext = os.path.splitext(filepath)
    output_path = f"{base}_{target_height}p{ext}"
    
    cmd = ['ffmpeg', '-y', '-i', filepath, '-vf', f"scale=-2:{target_height}", '-c:v', 'libx264', '-preset', 'superfast', '-crf', '28', '-c:a', 'copy', '-movflags', '+faststart', output_path]
    
    process = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    await process.communicate()
    
    if os.path.exists(output_path):
        os.remove(filepath) 
        return output_path
        
    return filepath

# --- INSTAGRAM HANDLER (USING INSTALOADER) ---
def download_instagram_content(url, output_file_path):
    """
    Downloads Instagram video using Instaloader logic to bypass yt-dlp blocks.
    """
    # Initialize Instaloader
    L = instaloader.Instaloader(
        download_pictures=False,
        download_videos=True,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False
    )
    
    # Extract shortcode from URL
    shortcode_match = re.search(r'(?:/p/|/reel/|/tv/)([\w-]+)', url)
    if not shortcode_match:
        raise Exception("Invalid Instagram URL")
    
    shortcode = shortcode_match.group(1)
    
    # Create a unique temp directory for this download
    temp_target = f"temp_insta_{shortcode}_{random.randint(1000,9999)}"
    
    try:
        post = instaloader.Post.from_shortcode(L.context, shortcode)
        L.download_post(post, target=temp_target)
        
        # Find the downloaded .mp4 file
        files = glob.glob(f"{temp_target}/*.mp4")
        if files:
            # Move the first found video to the desired output path
            shutil.move(files[0], output_file_path)
            return output_file_path
    except Exception as e:
        logging.error(f"Instaloader error: {e}")
    finally:
        # Cleanup temp directory
        if os.path.exists(temp_target):
            shutil.rmtree(temp_target, ignore_errors=True)
            
    return None

# --- MENUS ---
async def send_main_menu(event):
    user_id = event.sender_id
    mode = USER_DATA.get(user_id, {}).get('mode', 'normal')
    
    fast_lbl = "✅ Fast Mode ⚡" if mode == 'fast' else "Fast Mode ⚡"
    norm_lbl = "✅ Normal Mode 🐢" if mode == 'normal' else "Normal Mode 🐢"
    
    buttons = [
        [Button.text("Download Video", resize=True), Button.text("Screenshot")],
        [Button.text("Show Queue 📋")],
        [Button.text(fast_lbl), Button.text(norm_lbl)]
    ]
    await event.respond(f"🚀 **Super Bot Ready**\nLimit: **2GB per file**\nMode: **{mode.title()}**", buttons=buttons)

# --- HANDLERS ---

@client.on(events.NewMessage(pattern='/start'))
async def start(event):
    if is_auth(event.sender_id):
        await send_main_menu(event)
    else:
        await event.respond("🔒 **Locked.**", buttons=[[Button.text("Login", resize=True)]])

@client.on(events.NewMessage(pattern='Login'))
async def login_btn(event):
    if is_auth(event.sender_id): return await send_main_menu(event)
    if event.sender_id not in USER_DATA: USER_DATA[event.sender_id] = {}
    USER_DATA[event.sender_id]['state'] = 'waiting_password'
    await event.respond("🔑 **Enter Password:**", buttons=Button.clear())

@client.on(events.NewMessage(pattern='Fast Mode ⚡'))
async def set_fast(event):
    if not is_auth(event.sender_id): return
    if event.sender_id not in USER_DATA: USER_DATA[event.sender_id] = {}
    if USER_DATA[event.sender_id].get('mode') == 'fast': return await event.respond("⚠️ **Fast Mode is already active.**")
    USER_DATA[event.sender_id]['mode'] = 'fast'
    await event.respond("⚡ **Fast Mode (Aria2) Active**\nBest for: Direct MP4s, Large Files.")
    await send_main_menu(event)

@client.on(events.NewMessage(pattern='Normal Mode 🐢'))
async def set_normal(event):
    if not is_auth(event.sender_id): return
    if event.sender_id not in USER_DATA: USER_DATA[event.sender_id] = {}
    if USER_DATA[event.sender_id].get('mode') == 'normal': return await event.respond("⚠️ **Normal Mode is already active.**")
    USER_DATA[event.sender_id]['mode'] = 'normal'
    await event.respond("🐢 **Normal Mode (Native) Active**\nBest for: Instagram, YouTube, HLS.")
    await send_main_menu(event)

@client.on(events.NewMessage(pattern='Download Video'))
async def dl_btn(event):
    if not is_auth(event.sender_id): return
    USER_DATA[event.sender_id]['state'] = 'waiting_link'
    await event.respond("🚀 **Send Link:**", buttons=Button.clear())

@client.on(events.NewMessage(pattern='Screenshot'))
async def ss_btn(event):
    if not is_auth(event.sender_id): return
    USER_DATA[event.sender_id]['state'] = 'waiting_ss'
    await event.respond("📸 **Send Link for Screenshot:**", buttons=Button.clear())

@client.on(events.NewMessage(pattern='Show Queue 📋'))
async def show_queue_handler(event):
    if not is_auth(event.sender_id): return
    user_id = event.sender_id
    queue = USER_DATA.get(user_id, {}).get('queue', [])
    if not queue: return await event.respond("📭 **Queue is Empty.**")
    msg = "📋 **Current Queue:**\n\n"
    for i, item in enumerate(queue):
        msg += f"**{i+1}.** {item['title'][:40]}...\n   ├ Quality: {item['quality']}\n   └ Size: {item['size']}\n\n"
    await event.respond(msg)

@client.on(events.NewMessage(pattern='❌ Cancel Task'))
async def cancel_task_msg(event):
    # Fallback for text button (though we now use inline mostly)
    TASK_CANCEL[event.sender_id] = True
    await event.respond("🛑 **Stopping Current Task...**\n(Queue continues)")

# --- TEXT LISTENER ---
@client.on(events.NewMessage)
async def message_handler(event):
    text = event.text
    user_id = event.sender_id
    
    if text.startswith('/') or text in ['Login', 'Download Video', 'Screenshot', '❌ Cancel Task', 'Show Queue 📋'] or "Mode" in text:
        return

    if user_id not in USER_DATA: USER_DATA[user_id] = {}
    state = USER_DATA[user_id].get('state')

    if state == 'waiting_password':
        try: await event.delete()
        except: pass
        if text.strip() == SECRET_KEY:
            AUTHORIZED_USERS.add(user_id)
            USER_DATA[user_id]['state'] = None
            USER_DATA[user_id]['queue'] = []
            USER_DATA[user_id]['is_busy'] = False
            await event.respond("✅ **Access Granted.**")
            await send_main_menu(event)
        else:
            await event.respond("❌ **Wrong Password.**")
        return

    if state == 'waiting_ss':
        await process_screenshot(event, text)
        USER_DATA[user_id]['state'] = None
        return

    is_link = text.startswith("http")
    if (state == 'waiting_link' and is_link) or (is_auth(user_id) and is_link):
        USER_DATA[user_id]['state'] = None
        await process_link_quality(event, text)
        return

# --- CORE FUNCTIONS ---

async def process_screenshot(event, url):
    if not url.startswith("http"): url = "https://" + url
    msg = await event.respond("📸 **Capturing...**")
    try:
        api = f"https://s0.wp.com/mshots/v1/{requests.utils.quote(url)}?w=1920&h=1080"
        r = requests.get(api)
        f = io.BytesIO(r.content)
        f.name = "screenshot.jpg"
        await client.send_file(event.chat_id, f, caption=f"📸 {url}")
        await msg.delete()
    except Exception as e:
        await msg.edit(f"❌ Error: {e}")
    await send_main_menu(event)

async def process_link_quality(event, url):
    # --- INSTAGRAM BYPASS ---
    # We skip probing entirely for Instagram to avoid blocks
    if "instagram.com" in url:
        await run_download(event, url, "best")
        return

    msg = await event.respond("🔍 **Analyzing Link...**")
    
    if event.sender_id not in USER_DATA: USER_DATA[event.sender_id] = {}
    USER_DATA[event.sender_id]['current_url'] = url
    
    try:
        def get_info(ua_type="desktop"):
            ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            if ua_type == "mobile":
                ua = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'

            opts = {
                'quiet': True, 'no_warnings': True,
                'user_agent': ua,
                'referer': 'https://www.google.com/',
                'nocheckcertificate': True,
            }
            with yt_dlp.YoutubeDL(opts) as ydl: return ydl.extract_info(url, download=False)
        
        info = None
        # ATTEMPT 1: DESKTOP
        try:
            info = await asyncio.get_event_loop().run_in_executor(None, lambda: get_info("desktop"))
        except:
            pass 

        # ATTEMPT 2: MOBILE
        if not info or 'formats' not in info:
             info = await asyncio.get_event_loop().run_in_executor(None, lambda: get_info("mobile"))

        video_title = info.get('title', 'Unknown Video')
        USER_DATA[event.sender_id]['temp_title'] = video_title
        
        size_map = {}
        if info and 'formats' in info:
            for f in info['formats']:
                h = f.get('height')
                s = f.get('filesize') or f.get('filesize_approx')
                if h and s:
                    if s > size_map.get(h, 0): size_map[h] = s
        
        USER_DATA[event.sender_id]['size_map'] = size_map
        USER_DATA[event.sender_id]['best_size'] = info.get('filesize') or info.get('filesize_approx') or 0

        formats = []
        if info and 'formats' in info:
            seen = set()
            for f in info['formats']:
                h = f.get('height')
                if h and f.get('vcodec') != 'none' and h not in seen:
                    seen.add(h)
                    formats.append(h)
            formats.sort(reverse=True)
        
        standard_qualities = [1080, 720, 480, 360]
        display_qualities = sorted(list(set(standard_qualities) | set(formats)), reverse=True)
        display_qualities = [q for q in display_qualities if q >= 360 and q <= 2160]
        if not display_qualities: display_qualities = standard_qualities

        buttons = []
        row = []
        buttons.append([Button.inline("🌟 Default", data="q_best")])
        for res in display_qualities[:4]: 
            row.append(Button.inline(f"{res}p", data=f"q_{res}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row: buttons.append(row)
        buttons.append([Button.inline("❌ Cancel", data="cancel")])
        
        await msg.delete()
        await event.respond(f"🎞️ **Found:** `{video_title}`\n**Select Quality:**", buttons=buttons)
        
    except Exception as e:
        buttons = [[Button.inline("🌟 Default (Force)", data="q_best")], [Button.inline("❌ Cancel", data="cancel")]]
        await msg.delete()
        await event.respond("⚠️ **Could not probe quality.** Try Force Download:", buttons=buttons)

@client.on(events.CallbackQuery)
async def callback_handler(event):
    data = event.data.decode('utf-8')
    user_id = event.sender_id
    
    if data == 'cancel':
        await event.delete()
        return

    if data == 'cancel_task':
        TASK_CANCEL[user_id] = True
        await event.answer("🛑 Stopping...")
        return

    if data == 'show_q_active':
        queue = USER_DATA.get(user_id, {}).get('queue', [])
        if not queue:
            await event.answer("Empty Queue", alert=True)
        else:
            msg = "📋 Current Queue:\n"
            for i, item in enumerate(queue):
                msg += f"{i+1}. {item['title'][:20]}...\n"
            await event.answer(msg, alert=True)
        return

    if data.startswith('q_'):
        url = USER_DATA.get(user_id, {}).get('current_url')
        if not url: return await event.edit("❌ Link expired.")
        
        quality = data.split('_')[1]
        title = USER_DATA.get(user_id, {}).get('temp_title', 'Unknown Video')
        
        size_map = USER_DATA.get(user_id, {}).get('size_map', {})
        best_size = USER_DATA.get(user_id, {}).get('best_size', 0)
        target_size = 0
        if quality == 'best': target_size = best_size
        elif quality.isdigit(): target_size = size_map.get(int(quality), 0)
        if target_size == 0: target_size = best_size
        size_str = format_size(target_size) if target_size > 0 else "Unknown"

        await event.delete()
        
        if 'queue' not in USER_DATA[user_id]: USER_DATA[user_id]['queue'] = []
        
        USER_DATA[user_id]['queue'].append({
            'url': url,
            'quality': quality,
            'title': title,
            'size': size_str
        })
        
        q_len = len(USER_DATA[user_id]['queue'])
        await event.respond(f"✅ **Added to Queue** (Position: {q_len})\nTitle: `{title}`")
        asyncio.create_task(queue_worker(event, user_id))

async def run_download(event, url, quality):
    user_id = event.sender_id
    TASK_CANCEL[user_id] = False
    
    # NEW: Inline buttons
    task_buttons = [
        [Button.inline("📋 Show Queue", data="show_q_active"), Button.inline("❌ Cancel", data="cancel_task")]
    ]
    
    # --- DETECT INSTAGRAM & USE INSTALOADER ---
    if "instagram.com" in url:
        await safe_edit(await event.respond("Processing..."), "📸 **LINK INSTAGRAM DETECTED**", buttons=task_buttons)
        status_msg = await event.respond(f"🏎️ **Starting Engine (Instaloader)...**", buttons=task_buttons)
        
        timestamp = int(time.time())
        final_file = f"downloads/Instagram_{timestamp}.mp4"
        
        # Ensure dir
        if not os.path.exists("downloads"): os.makedirs("downloads")

        try:
            # RUN INSTALOADER LOGIC IN EXECUTOR TO AVOID BLOCKING
            downloaded_file = await asyncio.get_event_loop().run_in_executor(None, lambda: download_instagram_content(url, final_file))
            
            if downloaded_file and os.path.exists(downloaded_file):
                final_file = downloaded_file # Confirm path
                
                # Metadata extraction for upload
                file_size = os.path.getsize(final_file)
                status_msg = await safe_edit(status_msg, f"📤 **Uploading...**\nSize: {format_size(file_size)}", buttons=task_buttons)
                
                thumb_path = f"{final_file}.jpg"
                subprocess.run(['ffmpeg', '-y', '-i', final_file, '-ss', '00:00:01', '-vframes', '1', thumb_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                attributes = []
                try:
                    cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', final_file]
                    res = subprocess.run(cmd, capture_output=True, text=True)
                    dur = int(float(res.stdout.strip()))
                    cmd_wh = ['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=width,height', '-of', 'csv=p=0', final_file]
                    res_wh = subprocess.run(cmd_wh, capture_output=True, text=True)
                    w, h = map(int, res_wh.stdout.strip().split(','))
                    from telethon.tl.types import DocumentAttributeVideo
                    attributes = [DocumentAttributeVideo(duration=dur, w=w, h=h, supports_streaming=True)]
                except: pass

                # Upload Progress for Insta
                async def upload_progress_insta(current, total):
                    if TASK_CANCEL.get(user_id): raise Exception("Cancelled")
                    text = f"📤 **Uploading**\n{get_progress_bar(current, total, time.time())}" # Simple timer for simplicity here
                    # We avoid too many edits for simplicity in this specific block
                    if random.random() < 0.1: # Reduce edit frequency
                         if not client.loop.is_closed():
                            asyncio.run_coroutine_threadsafe(safe_edit(status_msg, text, buttons=task_buttons), client.loop)

                await client.send_file(
                    event.chat_id,
                    final_file,
                    caption="✅ **Download Complete**",
                    thumb=thumb_path if os.path.exists(thumb_path) else None,
                    progress_callback=upload_progress_insta,
                    attributes=attributes,
                    supports_streaming=True,
                    part_size_kb=512 
                )
                await status_msg.delete()
                os.remove(final_file)
                if os.path.exists(thumb_path): os.remove(thumb_path)
                print(f"INFO: Task Finished for User {user_id}")
                return # DONE WITH INSTA

            else:
                await safe_edit(status_msg, "❌ **Instagram Download Failed.**")
                return

        except Exception as e:
            await safe_edit(status_msg, f"❌ **Error:** {str(e)[:50]}")
            return

    # --- STANDARD LOGIC FOR EVERYTHING ELSE (xHamster, etc.) ---
    mode = USER_DATA.get(user_id, {}).get('mode', 'normal')
    mode_text = "⚡ Aria2" if mode == 'fast' else "🐢 Native"
    
    status_msg = await event.respond(f"🏎️ **Starting Engine ({mode_text})...**", buttons=task_buttons)
    
    timestamp = int(time.time())
    out_tmpl = f"downloads/{user_id}_{timestamp}_%(id)s.%(ext)s"
    
    target_height = None
    if quality == 'best':
        fmt = "bestvideo+bestaudio/best"
    else:
        target_height = int(quality)
        fmt = f"bestvideo[height<={target_height}]+bestaudio/best[height<={target_height}]/best"
    
    last_update = [0]
    start_time = [time.time()]
    
    def dl_progress(d):
        if TASK_CANCEL.get(user_id): raise yt_dlp.utils.DownloadError("Cancelled")
        if d['status'] == 'downloading':
            if time.time() - last_update[0] > 3:
                try:
                    total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
                    current = d.get('downloaded_bytes', 0)
                    text = f"📥 **Downloading ({mode_text})**\n{get_progress_bar(current, total, start_time[0])}"
                    if not client.loop.is_closed():
                        asyncio.run_coroutine_threadsafe(safe_edit(status_msg, text, buttons=task_buttons), client.loop)
                    last_update[0] = time.time()
                except: pass

    async def try_download(user_agent_type="desktop"):
        ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        if user_agent_type == "mobile":
            ua = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'

        ydl_opts = {
            'outtmpl': out_tmpl,
            'format': fmt,
            'quiet': True,
            'progress_hooks': [dl_progress],
            'noplaylist': True,
            'ignoreerrors': True,
            'no_warnings': True,
            'restrictfilenames': True,
            'nocheckcertificate': True,
            'user_agent': ua,
            'referer': 'https://www.google.com/',
            'noprogress': True, 
            'verbose': False, 
        }
        
        if mode == 'fast':
            ydl_opts['external_downloader'] = 'aria2c'
            ydl_opts['external_downloader_args'] = [
                '-x', '16', '-s', '16', '-k', '1M', 
                '--file-allocation=none', 
                '--max-connection-per-server=16',
                '--summary-interval=0', 
                '--console-log-level=warn' 
            ]
        
        def start_dl():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.extract_info(url, download=True)
        
        await asyncio.get_event_loop().run_in_executor(None, start_dl)

    try:
        print(f"INFO: Starting task for User {user_id} (Mode: {mode})")
        
        await try_download("desktop")
        
        files = glob.glob(f"downloads/{user_id}_{timestamp}_*")
        final_file = next((f for f in files if not f.endswith('.part')), None)
        
        if not final_file or not os.path.exists(final_file):
            await safe_edit(status_msg, "⚠️ **Retrying with Mobile Agent...**", buttons=task_buttons)
            await try_download("mobile")
            files = glob.glob(f"downloads/{user_id}_{timestamp}_*")
            final_file = next((f for f in files if not f.endswith('.part')), None)

        if final_file and os.path.exists(final_file):
            
            if target_height:
                final_file = await transcode_video(final_file, target_height, status_msg)
            else:
                final_file = await ensure_faststart(final_file, status_msg)

            safe_name = f"Video_{timestamp}_{random.randint(1000,9999)}.mp4"
            os.rename(final_file, safe_name)
            final_file = safe_name

            file_size = os.path.getsize(final_file)
            status_msg = await safe_edit(status_msg, f"📤 **Uploading...**\nSize: {format_size(file_size)}", buttons=task_buttons)
            
            attributes = []
            try:
                cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', final_file]
                res = subprocess.run(cmd, capture_output=True, text=True)
                dur = int(float(res.stdout.strip()))
                cmd_wh = ['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=width,height', '-of', 'csv=p=0', final_file]
                res_wh = subprocess.run(cmd_wh, capture_output=True, text=True)
                w, h = map(int, res_wh.stdout.strip().split(','))
                from telethon.tl.types import DocumentAttributeVideo
                attributes = [DocumentAttributeVideo(duration=dur, w=w, h=h, supports_streaming=True)]
            except: pass

            async def upload_progress(current, total):
                if TASK_CANCEL.get(user_id): raise Exception("Cancelled")
                if time.time() - last_update[0] > 4:
                    text = f"📤 **Uploading**\n{get_progress_bar(current, total, start_time[0])}"
                    await safe_edit(status_msg, text, buttons=task_buttons)
                    last_update[0] = time.time()

            thumb_path = f"{final_file}.jpg"
            subprocess.run(['ffmpeg', '-y', '-i', final_file, '-ss', '00:00:01', '-vframes', '1', thumb_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            await client.send_file(
                event.chat_id,
                final_file,
                caption="✅ **Download Complete**",
                thumb=thumb_path if os.path.exists(thumb_path) else None,
                progress_callback=upload_progress,
                attributes=attributes,
                supports_streaming=True,
                part_size_kb=512 
            )
            
            await status_msg.delete()
            os.remove(final_file)
            if os.path.exists(thumb_path): os.remove(thumb_path)
            print(f"INFO: Task Finished for User {user_id}")
            
        else:
            await safe_edit(status_msg, "❌ **Download Failed.** (Tried Desktop & Mobile)")
            print(f"WARN: Task Failed for User {user_id}")

    except Exception as e:
        if "Cancelled" in str(e):
            await safe_edit(status_msg, "🛑 **Cancelled.**")
        else:
            await safe_edit(status_msg, f"❌ **Error:** {str(e)[:100]}")

print("✅ Bot Running (Queue Fixed + Buttons Fixed + Instaloader Integrated)...")
client.run_until_disconnected()
