# bt/helpers/utils.py
# Updated version with video splitting functionality

import os
import uuid
import asyncio
from time import time
from PIL import Image
from logger import LOGGER
from typing import Optional, List
from asyncio.subprocess import PIPE
from asyncio import create_subprocess_exec, create_subprocess_shell, wait_for
from pyleaves import Leaves
from pyrogram.parser import Parser
from pyrogram.utils import get_channel_id
from pyrogram.types import (
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputMediaAudio,
    Voice,
)
from helpers.files import (
    fileSizeLimit,
    cleanup_download,
    get_readable_file_size
)
from helpers.msg import (
    get_parsed_msg
)

# Progress bar template
PROGRESS_BAR = """
Percentage: {percentage:.2f}% | {current}/{total}
Speed: {speed}/s
Estimated Time Left: {est_time} seconds
"""

async def cmd_exec(cmd, shell=False):
    if shell:
        proc = await create_subprocess_shell(cmd, stdout=PIPE, stderr=PIPE)
    else:
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = await proc.communicate()
    try:
        stdout = stdout.decode().strip()
    except:
        stdout = "Unable to decode the response!"
    try:
        stderr = stderr.decode().strip()
    except:
        stderr = "Unable to decode the error!"
    return stdout, stderr, proc.returncode

async def get_media_info(path):
    try:
        result = await cmd_exec([
            "ffprobe", "-hide_banner", "-loglevel", "error",
            "-print_format", "json", "-show_format", path,
        ])
    except Exception as e:
        print(f"Get Media Info: {e}. Mostly File not found! - File: {path}")
        return 0, None, None
    
    if result[0] and result[2] == 0:
        fields = eval(result[0]).get("format")
        if not fields:
            return 0, None, None
        duration = round(float(fields.get("duration", 0)))
        tags = fields.get("tags", {})
        artist = tags.get("artist") or tags.get("ARTIST") or tags.get("Artist")
        title = tags.get("title") or tags.get("TITLE") or tags.get("Title")
        return duration, artist, title
    return 0, None, None

async def split_large_video(video_path: str, progress_message) -> List[str]:
    """
    Split video larger than 2GB into parts using FFmpeg
    Returns list of part file paths
    """
    try:
        file_size = os.path.getsize(video_path)
        if file_size <= 2 * 1024 * 1024 * 1024:  # 2GB
            return []
        
        # Get video duration
        duration, _, _ = await get_media_info(video_path)
        if not duration:
            LOGGER(__name__).error("Could not get video duration for splitting")
            return []
        
        # Calculate number of parts needed (aim for ~1.8GB per part to be safe)
        target_size = 1.8 * 1024 * 1024 * 1024  # 1.8GB
        num_parts = max(2, int((file_size / target_size) + 0.5))
        part_duration = duration // num_parts
        
        LOGGER(__name__).info(f"Splitting {get_readable_file_size(file_size)} video into {num_parts} parts")
        
        # Get base filename without extension
        base_name = os.path.splitext(os.path.basename(video_path))[0]
        base_dir = os.path.dirname(video_path)
        
        part_paths = []
        
        for i in range(num_parts):
            start_time = i * part_duration
            
            # For the last part, go until the end
            if i == num_parts - 1:
                duration_arg = []  # No duration limit for last part
            else:
                duration_arg = ["-t", str(part_duration)]
            
            part_filename = f"{base_name}_part{i+1}.mp4"
            part_path = os.path.join(base_dir, part_filename)
            
            cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-ss", str(start_time), "-i", video_path,
                *duration_arg,
                "-c", "copy",  # Copy streams without re-encoding (faster)
                "-avoid_negative_ts", "make_zero",
                "-y", part_path
            ]
            
            await progress_message.edit(f"**✂️ Splitting video part {i+1}/{num_parts}...**")
            
            try:
                _, stderr, returncode = await wait_for(cmd_exec(cmd), timeout=300)  # 5 minute timeout
                
                if returncode != 0:
                    LOGGER(__name__).error(f"FFmpeg split error for part {i+1}: {stderr}")
                    # Clean up any partial files
                    for path in part_paths:
                        cleanup_download(path)
                    return []
                
                if os.path.exists(part_path) and os.path.getsize(part_path) > 0:
                    part_paths.append(part_path)
                    LOGGER(__name__).info(f"Created part {i+1}: {part_filename} ({get_readable_file_size(os.path.getsize(part_path))})")
                else:
                    LOGGER(__name__).error(f"Part {i+1} was not created or is empty")
                    # Clean up any partial files
                    for path in part_paths:
                        cleanup_download(path)
                    return []
                    
            except asyncio.TimeoutError:
                LOGGER(__name__).error(f"Timeout while splitting part {i+1}")
                # Clean up any partial files
                for path in part_paths:
                    cleanup_download(path)
                return []
        
        LOGGER(__name__).info(f"Successfully split video into {len(part_paths)} parts")
        return part_paths
        
    except Exception as e:
        LOGGER(__name__).error(f"Error splitting video: {e}")
        return []

async def get_video_thumbnail(video_file, duration):
    # Create truly unique thumbnail filename using UUID
    unique_id = str(uuid.uuid4())
    output = os.path.join("Assets", f"thumb_{unique_id}.jpg")
    
    # Ensure Assets directory exists
    os.makedirs("Assets", exist_ok=True)
    
    if duration is None:
        duration = (await get_media_info(video_file))[0]
    
    if not duration:
        duration = 3
    
    duration //= 2
    
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-ss", str(duration), "-i", video_file,
        "-vf", "thumbnail", "-q:v", "1", "-frames:v", "1",
        "-threads", str(os.cpu_count() // 2), "-y", output,
    ]
    
    try:
        _, err, code = await wait_for(cmd_exec(cmd, shell=False), timeout=60)
        if code != 0:
            LOGGER(__name__).error(f"FFmpeg error: {err}")
            return None
        
        if not os.path.exists(output):
            LOGGER(__name__).error(f"Thumbnail file not created: {output}")
            return None
        
        # Verify the file is not empty
        if os.path.getsize(output) == 0:
            LOGGER(__name__).error(f"Thumbnail file is empty: {output}")
            os.remove(output)
            return None
            
    except Exception as e:
        LOGGER(__name__).error(f"Thumbnail generation failed: {e}")
        return None
    
    return output

# Generate progress bar for downloading/uploading
def progressArgs(action: str, progress_message, start_time):
    return (action, progress_message, start_time, PROGRESS_BAR, "▓", "░")

async def processMediaGroup(chat_message, bot, message):
    media_group_messages = await chat_message.get_media_group()
    valid_media = []
    temp_paths = []
    invalid_paths = []
    thumbnail_paths = []  # Track thumbnail paths for cleanup
    
    start_time = time()
    progress_message = await message.reply("📥 Downloading media group...")
    
    LOGGER(__name__).info(
        f"Downloading media group with {len(media_group_messages)} items..."
    )
    
    # Process each media item sequentially to avoid conflicts
    for i, msg in enumerate(media_group_messages):
        if msg.photo or msg.video or msg.document or msg.audio:
            media_path = None
            try:
                LOGGER(__name__).info(f"Processing media {i+1}/{len(media_group_messages)}")
                
                # Generate unique filename for each media item
                from helpers.files import get_download_path
                from helpers.msg import get_file_name
                
                base_filename = get_file_name(msg.id, msg)  # Use individual message ID
                
                # Add index to filename to ensure uniqueness
                name, ext = os.path.splitext(base_filename)
                if not ext:
                    # Determine extension based on media type
                    if msg.video:
                        ext = ".mp4"
                    elif msg.photo:
                        ext = ".jpg"
                    elif msg.audio:
                        ext = ".mp3"
                    elif msg.document:
                        ext = msg.document.mime_type.split('/')[-1] if msg.document.mime_type else ""
                        if not ext.startswith('.'):
                            ext = f".{ext}" if ext else ""
                
                unique_filename = f"{name}_item{i+1}{ext}"
                download_path = get_download_path(message.id, unique_filename)
                
                media_path = await msg.download(
                    file_name=download_path,
                    progress=Leaves.progress_for_pyrogram,
                    progress_args=progressArgs(
                        f"📥 Downloading Progress ({i+1}/{len(media_group_messages)})",
                        progress_message,
                        start_time
                    ),
                )
                temp_paths.append(media_path)
                LOGGER(__name__).info(f"Downloaded: {media_path}")
                
                if msg.photo:
                    valid_media.append(
                        InputMediaPhoto(
                            media=media_path,
                            caption=await get_parsed_msg(
                                msg.caption or "", msg.caption_entities
                            ),
                        )
                    )
                elif msg.video:
                    # Check if video needs splitting
                    file_size = os.path.getsize(media_path)
                    if file_size > 2 * 1024 * 1024 * 1024:  # 2GB
                        LOGGER(__name__).info(f"Video {i+1} is larger than 2GB, splitting...")
                        await progress_message.edit(f"**✂️ Splitting large video {i+1}...**")
                        
                        split_paths = await split_large_video(media_path, progress_message)
                        if split_paths:
                            # Add each part as separate media
                            for j, part_path in enumerate(split_paths, 1):
                                temp_paths.append(part_path)
                                duration = (await get_media_info(part_path))[0]
                                thumb = await get_video_thumbnail(part_path, duration)
                                if thumb and os.path.exists(thumb):
                                    thumbnail_paths.append(thumb)
                                    with Image.open(thumb) as img:
                                        width, height = img.size
                                else:
                                    width, height = 480, 320
                                    thumb = None
                                
                                part_caption = f"{await get_parsed_msg(msg.caption or '', msg.caption_entities) or ''}\n**Part {j} of {len(split_paths)}**"
                                
                                valid_media.append(
                                    InputMediaVideo(
                                        media=part_path,
                                        thumb=thumb,
                                        width=width,
                                        height=height,
                                        duration=duration,
                                        caption=part_caption,
                                    )
                                )
                        else:
                            # Fallback to original if splitting failed
                            LOGGER(__name__).info(f"Generating thumbnail for video {i+1}")
                            duration = (await get_media_info(media_path))[0]
                            thumb = await get_video_thumbnail(media_path, duration)
                            if thumb and os.path.exists(thumb):
                                thumbnail_paths.append(thumb)
                                with Image.open(thumb) as img:
                                    width, height = img.size
                            else:
                                width, height = 480, 320
                                thumb = None
                            
                            valid_media.append(
                                InputMediaVideo(
                                    media=media_path,
                                    thumb=thumb,
                                    width=width,
                                    height=height,
                                    duration=duration,
                                    caption=await get_parsed_msg(
                                        msg.caption or "", msg.caption_entities
                                    ),
                                )
                            )
                    else:
                        LOGGER(__name__).info(f"Generating thumbnail for video {i+1}")
                        duration = (await get_media_info(media_path))[0]
                        thumb = await get_video_thumbnail(media_path, duration)
                        if thumb and os.path.exists(thumb):
                            thumbnail_paths.append(thumb)
                            try:
                                with Image.open(thumb) as img:
                                    width, height = img.size
                            except Exception as img_error:
                                LOGGER(__name__).error(f"Error reading thumbnail dimensions: {img_error}")
                                width, height = 480, 320
                        else:
                            width = 480
                            height = 320
                            thumb = None
                        
                        LOGGER(__name__).info(f"Video {i+1}: thumb={thumb}, duration={duration}, size={width}x{height}")
                        
                        valid_media.append(
                            InputMediaVideo(
                                media=media_path,
                                thumb=thumb,
                                width=width,
                                height=height,
                                duration=duration,
                                caption=await get_parsed_msg(
                                    msg.caption or "", msg.caption_entities
                                ),
                            )
                        )
                elif msg.document:
                    valid_media.append(
                        InputMediaDocument(
                            media=media_path,
                            caption=await get_parsed_msg(
                                msg.caption or "", msg.caption_entities
                            ),
                        )
                    )
                elif msg.audio:
                    duration, artist, title = await get_media_info(media_path)
                    valid_media.append(
                        InputMediaAudio(
                            media=media_path,
                            duration=duration,
                            performer=artist,
                            title=title,
                            caption=await get_parsed_msg(
                                msg.caption or "", msg.caption_entities
                            ),
                        )
                    )
            except Exception as e:
                LOGGER(__name__).error(f"Error processing media {i+1}: {e}")
                if media_path and os.path.exists(media_path):
                    invalid_paths.append(media_path)
                continue
    
    LOGGER(__name__).info(f"Valid media count: {len(valid_media)}")
    
    if valid_media:
        try:
            LOGGER(__name__).info("Sending media group...")
            
            # If media group is too large, send in chunks of 10 (Telegram limit)
            chunk_size = 10
            for i in range(0, len(valid_media), chunk_size):
                chunk = valid_media[i:i + chunk_size]
                await bot.send_media_group(chat_id=message.chat.id, media=chunk)
                if i + chunk_size < len(valid_media):
                    await asyncio.sleep(1)  # Small delay between chunks
            
            LOGGER(__name__).info("Media group sent successfully")
            await progress_message.delete()
            
        except Exception as e:
            LOGGER(__name__).error(f"Failed to send media group: {e}")
            await message.reply(
                "**❌ Failed to send media group, trying individual uploads**"
            )
            
            # Send each media individually with proper parameters
            for i, media in enumerate(valid_media):
                try:
                    LOGGER(__name__).info(f"Sending individual media {i+1}/{len(valid_media)}")
                    if isinstance(media, InputMediaPhoto):
                        await bot.send_photo(
                            chat_id=message.chat.id,
                            photo=media.media,
                            caption=media.caption,
                        )
                    elif isinstance(media, InputMediaVideo):
                        await bot.send_video(
                            chat_id=message.chat.id,
                            video=media.media,
                            thumb=media.thumb,
                            width=media.width,
                            height=media.height,
                            duration=media.duration,
                            caption=media.caption,
                        )
                    elif isinstance(media, InputMediaDocument):
                        await bot.send_document(
                            chat_id=message.chat.id,
                            document=media.media,
                            caption=media.caption,
                        )
                    elif isinstance(media, InputMediaAudio):
                        await bot.send_audio(
                            chat_id=message.chat.id,
                            audio=media.media,
                            duration=media.duration,
                            performer=media.performer,
                            title=media.title,
                            caption=media.caption,
                        )
                    await asyncio.sleep(0.5)  # Small delay between individual sends
                except Exception as individual_e:
                    LOGGER(__name__).error(f"Failed to upload individual media {i+1}: {individual_e}")
            
            await progress_message.delete()
        
        # Cleanup all downloaded files and thumbnails
        LOGGER(__name__).info(f"Cleaning up {len(temp_paths + invalid_paths + thumbnail_paths)} files")
        for path in temp_paths + invalid_paths + thumbnail_paths:
            cleanup_download(path)
        
        return True
    
    await progress_message.delete()
    await message.reply("❌ No valid media found in the media group.")
    for path in invalid_paths + thumbnail_paths:
        cleanup_download(path)
    return False

async def send_media(
    bot, message, media_path, media_type, caption, progress_message, start_time
):
    file_size = os.path.getsize(media_path)
    if not await fileSizeLimit(file_size, message, "upload"):
        return
    
    progress_args = progressArgs("📥 Uploading Progress", progress_message, start_time)
    LOGGER(__name__).info(f"Uploading media: {media_path} ({media_type})")
    
    if media_type == "photo":
        await message.reply_photo(
            media_path,
            caption=caption or "",
            progress=Leaves.progress_for_pyrogram,
            progress_args=progress_args,
        )
    elif media_type == "video":
        # Remove old generic thumbnail if exists
        old_thumb_pattern = os.path.join("Assets", "video_thumb.jpg")
        if os.path.exists(old_thumb_pattern):
            os.remove(old_thumb_pattern)
        
        duration = (await get_media_info(media_path))[0]
        thumb = await get_video_thumbnail(media_path, duration)
        
        if thumb is not None and thumb != "none":
            with Image.open(thumb) as img:
                width, height = img.size
        else:
            width = 480
            height = 320
        
        if thumb == "none":
            thumb = None
        
        await message.reply_video(
            media_path,
            duration=duration,
            width=width,
            height=height,
            thumb=thumb,
            caption=caption or "",
            progress=Leaves.progress_for_pyrogram,
            progress_args=progress_args,
        )
        
        # Clean up the unique thumbnail after upload
        if thumb and os.path.exists(thumb):
            cleanup_download(thumb)
            
    elif media_type == "audio":
        duration, artist, title = await get_media_info(media_path)
        await message.reply_audio(
            media_path,
            duration=duration,
            performer=artist,
            title=title,
            caption=caption or "",
            progress=Leaves.progress_for_pyrogram,
            progress_args=progress_args,
        )
    elif media_type == "document":
        await message.reply_document(
            media_path,
            caption=caption or "",
            progress=Leaves.progress_for_pyrogram,
            progress_args=progress_args,
        )