# bt/main.py
# Updated version with Telethon integration and video splitting

import os
import shutil
import psutil
import asyncio
from time import time
from pyleaves import Leaves
from pyrogram.enums import ParseMode
from pyrogram import Client, filters
from pyrogram.errors import PeerIdInvalid, BadRequest
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from helpers.utils import (
    processMediaGroup,
    progressArgs,
    send_media,
    split_large_video  # New function for video splitting
)
from helpers.files import (
    get_download_path,
    fileSizeLimit,
    get_readable_file_size,
    get_readable_time,
    cleanup_download
)
from helpers.msg import (
    getChatMsgID,
    get_file_name,
    get_parsed_msg
)
from helpers.telethon_client import telethon_handler  # New import
from config import PyroConf
from logger import LOGGER

from helpers.downloaders import (
    save_cookies,
    aria2c_download,
    ytdlp_download,
    split_file_p7zip
)


# Initialize the bot client
bot = Client(
    "media_bot",
    api_id=PyroConf.API_ID,
    api_hash=PyroConf.API_HASH,
    bot_token=PyroConf.BOT_TOKEN,
    workers=1000,
    parse_mode=ParseMode.MARKDOWN,
)

# Client for user session
user = Client("user_session", workers=1000, session_string=PyroConf.SESSION_STRING)

RUNNING_TASKS = set()

def track_task(coro):
    task = asyncio.create_task(coro)
    RUNNING_TASKS.add(task)
    def _remove(_):
        RUNNING_TASKS.discard(task)
    task.add_done_callback(_remove)
    return task

@bot.on_message(filters.command("start") & filters.private)
async def start(_, message: Message):
    welcome_text = (
        "👋 **Welcome to Media Downloader Bot!**\n\n"
        "I can grab photos, videos, audio, and documents from any Telegram post.\n"
        "Just send me a link (paste it directly or use `/dl <link>`),\n"
        "or reply to a message with `/dl`.\n\n"
        "ℹ️ Use `/help` to view all commands and examples.\n"
        "🔒 Make sure the user client is part of the chat.\n\n"
        "Ready? Send me a Telegram post link!"
    )
    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Update Channel", url="https://t.me/itsSmartDev")]]
    )
    await message.reply(welcome_text, reply_markup=markup, disable_web_page_preview=True)

# Add these new command handlers to bt/main.py after the existing commands
# Insert these after the existing command handlers (around line 100)

from helpers.downloaders import (
    save_cookies,
    aria2c_download,
    ytdlp_download,
    split_file_p7zip
)

@bot.on_message(filters.command("ck") & filters.private)
async def save_cookies_command(_, message: Message):
    """Save cookies in Netscape format for yt-dlp"""
    if len(message.text.split(None, 1)) < 2:
        await message.reply(
            "**🍪 Cookie Manager**\n\n"
            "Send cookies in Netscape format:\n"
            "`/ck [paste cookies here]`\n\n"
            "The cookies will be saved and used for YouTube downloads.\n"
            "New cookies will replace old ones."
        )
        return
    
    cookies_text = message.text.split(None, 1)[1]
    
    if await save_cookies(cookies_text):
        await message.reply("✅ **Cookies saved successfully!**\nThey will be used for YouTube downloads.")
    else:
        await message.reply("❌ **Failed to save cookies. Please check the format.**")

@bot.on_message(filters.command("l") & filters.private)
async def aria2c_download_command(bot: Client, message: Message):
    """Download files with aria2c"""
    if len(message.text.split(None, 1)) < 2:
        await message.reply(
            "**⬇️ Aria2c Downloader**\n\n"
            "Download files using aria2c:\n"
            "`/l URL` or `/l URL1 URL2 ...`\n\n"
            "Features:\n"
            "• 16 connections per server\n"
            "• Auto-split large files\n"
            "• Files >2GB will be split with 7zip"
        )
        return
    
    urls_text = message.text.split(None, 1)[1]
    urls = urls_text.split()
    
    progress_message = await message.reply("**🔍 Processing download links...**")
    
    for i, url in enumerate(urls, 1):
        try:
            await progress_message.edit(f"**📥 Downloading file {i}/{len(urls)}...**\n{url[:50]}...")
            
            # Generate unique filename
            import datetime
            from urllib.parse import urlparse, unquote
            from helpers.downloaders import is_video_file  # Import the new function
            
            parsed_url = urlparse(url)
            filename = unquote(os.path.basename(parsed_url.path)) or f"download_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
            download_path = get_download_path(message.id, filename)
            
            # Download with aria2c
            success, result = await aria2c_download(url, download_path)
            
            if not success:
                await message.reply(f"❌ **Failed to download file {i}:**\n{result}")
                continue
            
            file_size = os.path.getsize(result)
            LOGGER(__name__).info(f"Downloaded file size: {get_readable_file_size(file_size)}")
            
            # Use filename as caption
            caption = f"**{filename}**"
            
            # Check if it's a video file (including MP4)
            is_video = is_video_file(result)
            
            # Check if file needs splitting (>2GB)
            if file_size > 2 * 1024 * 1024 * 1024:
                if is_video:
                    # Use video splitting for video files
                    await progress_message.edit(f"**✂️ Video >2GB, splitting...**")
                    
                    from helpers.utils import split_large_video, get_media_info, get_video_thumbnail
                    parts = await split_large_video(result, progress_message)
                    
                    if parts:
                        # Upload each part as video
                        for j, part_path in enumerate(parts, 1):
                            await progress_message.edit(f"**📤 Uploading part {j}/{len(parts)}...**")
                            
                            duration, _, _ = await get_media_info(part_path)
                            thumb = await get_video_thumbnail(part_path, duration)
                            
                            part_caption = f"**{filename}**\n**Part {j} of {len(parts)}**"
                            
                            await message.reply_video(
                                part_path,
                                duration=duration,
                                thumb=thumb,
                                caption=part_caption,
                                progress=Leaves.progress_for_pyrogram,
                                progress_args=progressArgs(
                                    f"📤 Uploading Part {j}/{len(parts)}",
                                    progress_message,
                                    time()
                                )
                            )
                            
                            cleanup_download(part_path)
                            if thumb:
                                cleanup_download(thumb)
                        
                        cleanup_download(result)
                    else:
                        # If video splitting failed, try 7zip
                        await progress_message.edit(f"**✂️ Splitting with 7zip...**")
                        parts = await split_file_p7zip(result, max_size_mb=1900, progress_message=progress_message)
                        
                        if parts:
                            for j, part_path in enumerate(parts, 1):
                                await progress_message.edit(f"**📤 Uploading part {j}/{len(parts)}...**")
                                await message.reply_document(
                                    part_path,
                                    caption=f"**{filename}**\n**Archive Part {j}/{len(parts)}**\nExtract all parts to get the video.",
                                    progress=Leaves.progress_for_pyrogram,
                                    progress_args=progressArgs(f"📤 Part {j}", progress_message, time())
                                )
                                cleanup_download(part_path)
                            cleanup_download(result)
                        else:
                            # Upload as is
                            await _upload_video_or_doc(bot, message, result, filename, progress_message)
                else:
                    # Non-video file, use 7zip
                    await progress_message.edit(f"**✂️ File >2GB, splitting with 7zip...**")
                    parts = await split_file_p7zip(result, max_size_mb=1900, progress_message=progress_message)
                    
                    if parts:
                        for j, part_path in enumerate(parts, 1):
                            await progress_message.edit(f"**📤 Uploading part {j}/{len(parts)}...**")
                            await message.reply_document(
                                part_path,
                                caption=f"**{filename}**\n**Part {j} of {len(parts)}**",
                                progress=Leaves.progress_for_pyrogram,
                                progress_args=progressArgs(f"📤 Part {j}", progress_message, time())
                            )
                            cleanup_download(part_path)
                        cleanup_download(result)
                    else:
                        await _upload_video_or_doc(bot, message, result, filename, progress_message)
            else:
                # Upload directly with proper type detection
                await _upload_video_or_doc(bot, message, result, filename, progress_message)
                
        except Exception as e:
            LOGGER(__name__).error(f"Error downloading {url}: {e}")
            await message.reply(f"❌ **Error with file {i}:** {str(e)}")
    
    await progress_message.delete()
    await message.reply(f"✅ **Completed processing {len(urls)} link(s)**")

@bot.on_message(filters.command("yl") & filters.private)
async def ytdlp_download_command(bot: Client, message: Message):
    """Download videos with yt-dlp"""
    if len(message.text.split(None, 1)) < 2:
        await message.reply(
            "**📹 YouTube Downloader (yt-dlp)**\n\n"
            "Download videos using yt-dlp:\n"
            "`/yl URL` or `/yl URL1 URL2 ...`\n\n"
            "Features:\n"
            "• Uses saved cookies (if available)\n"
            "• aria2c with 16 connections\n"
            "• Auto-split videos >2GB\n"
            "• Uses video title as caption"
        )
        return
    
    urls_text = message.text.split(None, 1)[1]
    urls = urls_text.split()
    
    progress_message = await message.reply("**🔍 Processing video links...**")
    
    for i, url in enumerate(urls, 1):
        try:
            await progress_message.edit(f"**📥 Downloading video {i}/{len(urls)}...**\n{url[:50]}...")
            
            # Generate unique filename (will be updated after download)
            import datetime
            temp_filename = f"video_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4"
            download_path = get_download_path(message.id, temp_filename)
            
            # Download with yt-dlp (now returns 3 values: success, path, title)
            success, result, video_title = await ytdlp_download(url, download_path, use_aria2c=True, progress_message=progress_message)
            
            if not success:
                await message.reply(f"❌ **Failed to download video {i}:**\n{result}")
                continue
            
            # Get actual filename from downloaded file
            actual_filename = os.path.basename(result)
            file_size = os.path.getsize(result)
            LOGGER(__name__).info(f"Downloaded video: {actual_filename}, size: {get_readable_file_size(file_size)}")
            
            # Use video title as caption, fallback to filename if no title
            if video_title:
                caption = f"**{video_title}**"
                LOGGER(__name__).info(f"Using video title as caption: {video_title}")
            else:
                caption = f"**{actual_filename}**"
                LOGGER(__name__).info(f"No title found, using filename as caption: {actual_filename}")
            
            # Check if it's a video file
            from helpers.downloaders import is_video_file
            is_video = is_video_file(result)
            
            # Check if file needs splitting (>2GB)
            if file_size > 2 * 1024 * 1024 * 1024:
                if is_video:
                    # Use video splitting method
                    await progress_message.edit(f"**✂️ Video >2GB, splitting...**")
                    
                    from helpers.utils import split_large_video
                    parts = await split_large_video(result, progress_message)
                    
                    if parts:
                        # Upload each part as video
                        for j, part_path in enumerate(parts, 1):
                            await progress_message.edit(f"**📤 Uploading part {j}/{len(parts)}...**")
                            
                            # Get video info
                            from helpers.utils import get_media_info, get_video_thumbnail
                            duration, _, _ = await get_media_info(part_path)
                            thumb = await get_video_thumbnail(part_path, duration)
                            
                            # Include title in part caption
                            if video_title:
                                part_caption = f"**{video_title}**\n**Part {j} of {len(parts)}**"
                            else:
                                part_caption = f"**{actual_filename}**\n**Part {j} of {len(parts)}**"
                            
                            await message.reply_video(
                                part_path,
                                duration=duration,
                                thumb=thumb,
                                caption=part_caption,
                                progress=Leaves.progress_for_pyrogram,
                                progress_args=progressArgs(
                                    f"📤 Uploading Part {j}/{len(parts)}",
                                    progress_message,
                                    time()
                                )
                            )
                            
                            cleanup_download(part_path)
                            if thumb:
                                cleanup_download(thumb)
                        
                        cleanup_download(result)
                    else:
                        # If video splitting failed, try 7zip
                        await progress_message.edit(f"**✂️ Splitting with 7zip...**")
                        parts = await split_file_p7zip(result, max_size_mb=1900, progress_message=progress_message)
                        
                        if parts:
                            for j, part_path in enumerate(parts, 1):
                                await progress_message.edit(f"**📤 Uploading part {j}/{len(parts)}...**")
                                
                                # Use title in archive caption
                                if video_title:
                                    archive_caption = f"**{video_title}**\n**Video Archive Part {j}/{len(parts)}**\nExtract all parts to get the video."
                                else:
                                    archive_caption = f"**{actual_filename}**\n**Video Archive Part {j}/{len(parts)}**\nExtract all parts to get the video."
                                
                                await message.reply_document(
                                    part_path,
                                    caption=archive_caption,
                                    progress=Leaves.progress_for_pyrogram,
                                    progress_args=progressArgs(f"📤 Part {j}", progress_message, time())
                                )
                                cleanup_download(part_path)
                            cleanup_download(result)
                        else:
                            # Upload as is with title
                            await _upload_video_or_doc_with_caption(bot, message, result, caption, progress_message)
                else:
                    # Non-video file, use 7zip
                    await progress_message.edit(f"**✂️ File >2GB, splitting with 7zip...**")
                    parts = await split_file_p7zip(result, max_size_mb=1900, progress_message=progress_message)
                    
                    if parts:
                        for j, part_path in enumerate(parts, 1):
                            await progress_message.edit(f"**📤 Uploading part {j}/{len(parts)}...**")
                            
                            # Use title for non-video parts too
                            if video_title:
                                part_caption = f"**{video_title}**\n**Part {j} of {len(parts)}**"
                            else:
                                part_caption = f"**{actual_filename}**\n**Part {j} of {len(parts)}**"
                            
                            await message.reply_document(
                                part_path,
                                caption=part_caption,
                                progress=Leaves.progress_for_pyrogram,
                                progress_args=progressArgs(f"📤 Part {j}", progress_message, time())
                            )
                            cleanup_download(part_path)
                        cleanup_download(result)
                    else:
                        await _upload_video_or_doc_with_caption(bot, message, result, caption, progress_message)
            else:
                # Upload directly with title as caption
                await _upload_video_or_doc_with_caption(bot, message, result, caption, progress_message)
                
        except Exception as e:
            LOGGER(__name__).error(f"Error downloading {url}: {e}")
            await message.reply(f"❌ **Error with video {i}:** {str(e)}")
    
    await progress_message.delete()
    await message.reply(f"✅ **Completed processing {len(urls)} video(s)**")

async def _upload_video_or_doc_with_caption(bot, message, file_path, caption, progress_message):
    """Helper to upload video or document with custom caption"""
    from helpers.downloaders import is_video_file
    from helpers.utils import get_media_info, get_video_thumbnail
    
    file_size = os.path.getsize(file_path)
    is_video = is_video_file(file_path)
    
    if is_video:
        # Upload as video (streamable)
        await progress_message.edit("**📤 Uploading video...**")
        duration, _, _ = await get_media_info(file_path)
        thumb = await get_video_thumbnail(file_path, duration)
        
        await message.reply_video(
            file_path,
            duration=duration,
            thumb=thumb,
            caption=caption,
            progress=Leaves.progress_for_pyrogram,
            progress_args=progressArgs("📤 Uploading Video", progress_message, time())
        )
        
        if thumb:
            cleanup_download(thumb)
    else:
        # Upload as document (for non-video files)
        await progress_message.edit("**📤 Uploading file...**")
        await message.reply_document(
            file_path,
            caption=caption,
            progress=Leaves.progress_for_pyrogram,
            progress_args=progressArgs("📤 Uploading File", progress_message, time())
        )
    
    cleanup_download(file_path)

async def _upload_video_or_doc(bot, message, file_path, filename, progress_message):
    """Helper to upload video or document based on file type - always sends MP4 as video"""
    from helpers.downloaders import is_video_file
    from helpers.utils import get_media_info, get_video_thumbnail
    
    file_size = os.path.getsize(file_path)
    is_video = is_video_file(file_path)
    
    # Use filename as caption
    caption = f"**{filename}**"
    
    if is_video:
        # Upload as video (streamable)
        await progress_message.edit("**📤 Uploading video...**")
        duration, _, _ = await get_media_info(file_path)
        thumb = await get_video_thumbnail(file_path, duration)
        
        await message.reply_video(
            file_path,
            duration=duration,
            thumb=thumb,
            caption=caption,
            progress=Leaves.progress_for_pyrogram,
            progress_args=progressArgs("📤 Uploading Video", progress_message, time())
        )
        
        if thumb:
            cleanup_download(thumb)
    else:
        # Upload as document (for non-video files)
        await progress_message.edit("**📤 Uploading file...**")
        await message.reply_document(
            file_path,
            caption=caption,
            progress=Leaves.progress_for_pyrogram,
            progress_args=progressArgs("📤 Uploading File", progress_message, time())
        )
    
    cleanup_download(file_path)


@bot.on_message(filters.command("help") & filters.private)
async def help_command(_, message: Message):
    help_text = (
        "💡 **Media Downloader Bot Help**\n\n"
        "➤ **Download Media**\n"
        " – Send `/dl <post_URL>` **or** just paste a Telegram post link to fetch photos, videos, audio, or documents.\n\n"
        "➤ **Batch Download**\n"
        " – Send `/bdl start_link end_link` to grab a series of posts in one go.\n"
        " 💡 Example: `/bdl https://t.me/mychannel/100 https://t.me/mychannel/120`\n"
        "**It will download all posts from ID 100 to 120.**\n\n"
        "➤ **Requirements**\n"
        " – Make sure the user client is part of the chat.\n\n"
        "➤ **If the bot hangs**\n"
        " – Send `/killall` to cancel any pending downloads.\n\n"
        "➤ **Logs**\n"
        " – Send `/logs` to download the bot's logs file.\n\n"
        "➤ **Stats**\n"
        " – Send `/stats` to view current status:\n\n"
        "**Example**:\n"
        " • `/dl https://t.me/itsSmartDev/547`\n"
        " • `https://t.me/itsSmartDev/547`"
    )
    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Update Channel", url="https://t.me/itsSmartDev")]]
    )
    await message.reply(help_text, reply_markup=markup, disable_web_page_preview=True)

async def handle_download(bot: Client, message: Message, post_url: str):
    # Cut off URL at '?' if present
    if "?" in post_url:
        post_url = post_url.split("?", 1)[0]
    
    try:
        chat_id, message_thread_id, message_id = getChatMsgID(post_url)
        
        # Get the message normally (Pyrogram doesn't support message_thread_id parameter)
        chat_message = await user.get_messages(chat_id=chat_id, message_ids=message_id)
        
        # If this is supposed to be a forum topic message, verify it belongs to the topic
        if message_thread_id:
            if not message_belongs_to_topic(chat_message, message_thread_id):
                await message.reply(
                    f"**❌ Message {message_id} does not belong to topic {message_thread_id} or has been deleted.**\n"
                    f"**Original URL:** {post_url}"
                )
                return
        
        LOGGER(__name__).info(f"Downloading media from URL: {post_url}")
        
        if chat_message.document or chat_message.video or chat_message.audio:
            file_size = (
                chat_message.document.file_size
                if chat_message.document
                else chat_message.video.file_size
                if chat_message.video
                else chat_message.audio.file_size
            )
            
            if not await fileSizeLimit(
                file_size, message, "download", user.me.is_premium
            ):
                return
        
        parsed_caption = await get_parsed_msg(
            chat_message.caption or "", chat_message.caption_entities
        )
        parsed_text = await get_parsed_msg(
            chat_message.text or "", chat_message.entities
        )
        
        if chat_message.media_group_id:
            if not await processMediaGroup(chat_message, bot, message):
                await message.reply(
                    "**Could not extract any valid media from the media group.**"
                )
            return
        elif chat_message.media:
            start_time = time()
            progress_message = await message.reply("**📥 Downloading Progress...**")
            
            # Generate unique filename with timestamp to avoid conflicts
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = get_file_name(message_id, chat_message)
            name, ext = os.path.splitext(base_filename)
            unique_filename = f"{name}_{timestamp}{ext}"
            
            download_path = get_download_path(message.id, unique_filename)
            
            media_path = await chat_message.download(
                file_name=download_path,
                progress=Leaves.progress_for_pyrogram,
                progress_args=progressArgs(
                    "📥 Downloading Progress", progress_message, start_time
                ),
            )
            
            LOGGER(__name__).info(f"Downloaded media: {media_path}")
            
            media_type = (
                "photo"
                if chat_message.photo
                else "video"
                if chat_message.video
                else "audio"
                if chat_message.audio
                else "document"
            )
            
            # Check if video is larger than 2GB and split if needed
            if media_type == "video" and os.path.getsize(media_path) > 2 * 1024 * 1024 * 1024:
                LOGGER(__name__).info(f"Video file is larger than 2GB, splitting...")
                await progress_message.edit("**✂️ Splitting large video...**")
                
                split_paths = await split_large_video(media_path, progress_message)
                if split_paths:
                    # Upload each part
                    for i, part_path in enumerate(split_paths, 1):
                        part_caption = f"{parsed_caption or ''}\n**Part {i} of {len(split_paths)}**" if parsed_caption else f"**Part {i} of {len(split_paths)}**"
                        await send_media(
                            bot,
                            message,
                            part_path,
                            media_type,
                            part_caption,
                            progress_message,
                            start_time,
                        )
                        cleanup_download(part_path)
                else:
                    # Fallback to original file if splitting failed
                    await send_media(
                        bot,
                        message,
                        media_path,
                        media_type,
                        parsed_caption,
                        progress_message,
                        start_time,
                    )
            else:
                await send_media(
                    bot,
                    message,
                    media_path,
                    media_type,
                    parsed_caption,
                    progress_message,
                    start_time,
                )
            
            cleanup_download(media_path)
            await progress_message.delete()
        elif chat_message.text or chat_message.caption:
            await message.reply(parsed_text or parsed_caption)
        else:
            await message.reply("**No media or text found in the post URL.**")
    
    except (PeerIdInvalid, BadRequest, KeyError):
        await message.reply("**Make sure the user client is part of the chat.**")
    except Exception as e:
        error_message = f"**❌ {str(e)}**"
        await message.reply(error_message)
        LOGGER(__name__).error(e)

def message_belongs_to_topic(message, topic_id: int) -> bool:
    """Check if a message belongs to a specific forum topic"""
    if not message or message.empty:
        return False
    
    # Check if this is the topic starter message
    if message.id == topic_id:
        return True
    
    # Primary check: message_thread_id (this is the main field for forum topics)
    if hasattr(message, 'message_thread_id') and message.message_thread_id == topic_id:
        return True
    
    # Alternative check: reply_to_message_id for older format
    if hasattr(message, 'reply_to_message_id') and message.reply_to_message_id == topic_id:
        return True
    
    # Check for top_id (forum topics)
    if hasattr(message, 'reply_to') and hasattr(message.reply_to, 'reply_to_top_id') and message.reply_to.reply_to_top_id == topic_id:
        return True
    
    # Check if message is part of forum topic using other indicators
    if hasattr(message, 'forum_topic_created') and message.id == topic_id:
        return True
    
    return False

@bot.on_message(filters.command("dl") & filters.private)
async def download_media(bot: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("**Provide a post URL after the /dl command.**")
        return
    
    post_url = message.command[1]
    await track_task(handle_download(bot, message, post_url))

@bot.on_message(filters.command("bdl") & filters.private)
async def download_range(bot: Client, message: Message):
    args = message.text.split()
    if len(args) != 3 or not all(arg.startswith("https://t.me/") for arg in args[1:]):
        await message.reply(
            "🚀 **Batch Download Process**\n"
            "`/bdl start_link end_link`\n\n"
            "💡 **Example:**\n"
            "`/bdl https://t.me/mychannel/100 https://t.me/mychannel/120`\n"
            "`/bdl https://t.me/channel/topic/100 https://t.me/channel/topic/120`"
        )
        return
    
    try:
        start_chat, start_thread, start_id = getChatMsgID(args[1])
        end_chat, end_thread, end_id = getChatMsgID(args[2])
    except Exception as e:
        return await message.reply(f"**❌ Error parsing links:\n{e}**")
    
    if start_chat != end_chat:
        return await message.reply("**❌ Both links must be from the same channel.**")
    
    if start_thread != end_thread:
        return await message.reply("**❌ Both links must be from the same topic thread.**")
    
    if start_id > end_id:
        return await message.reply("**❌ Invalid range: start ID cannot exceed end ID.**")
    
    try:
        await user.get_chat(start_chat)
    except Exception:
        pass
    
    # Build the correct URL prefix based on whether it's a forum topic or not
    if start_thread:
        prefix = args[1].rsplit("/", 1)[0]  # Keep the topic thread in URL
        batch_type = f"forum topic {start_thread} posts"
        
        # Use Telethon to get the exact message IDs in the topic
        loading = await message.reply(f"🔍 **Getting message IDs from topic {start_thread}...**")
        
        message_ids = await telethon_handler.get_topic_messages_range(
            start_chat, start_thread, start_id, end_id
        )
        
        if not message_ids:
            await loading.delete()
            return await message.reply(
                f"**❌ No messages found in topic {start_thread} between {start_id} and {end_id}.**\n"
                "Make sure the Telethon session is valid and has access to the chat."
            )
        
        await loading.edit(f"📥 **Downloading {len(message_ids)} {batch_type} {start_id}–{end_id}…**")
        
    else:
        prefix = args[1].rsplit("/", 1)[0]
        batch_type = "posts"
        message_ids = list(range(start_id, end_id + 1))  # Sequential for non-forum
        loading = await message.reply(f"📥 **Downloading {batch_type} {start_id}–{end_id}…**")
    
    downloaded = skipped = failed = 0
    deleted_messages = []
    not_in_topic = []
    processed_media_groups = set()  # Track processed media group IDs
    media_group_skipped = []  # Track message IDs skipped due to media group
    
    for msg_id in message_ids:
        url = f"{prefix}/{msg_id}"
        try:
            # Get message normally (no message_thread_id parameter)
            chat_msg = await user.get_messages(chat_id=start_chat, message_ids=msg_id)
            
            if not chat_msg or chat_msg.empty:
                deleted_messages.append(msg_id)
                skipped += 1
                continue
            
            # For forum topics, we already filtered with Telethon, but double-check
            if start_thread and not message_belongs_to_topic(chat_msg, start_thread):
                not_in_topic.append(msg_id)
                skipped += 1
                continue
            
            # Check if this message is part of a media group
            if chat_msg.media_group_id:
                if chat_msg.media_group_id in processed_media_groups:
                    # This media group was already processed, skip this message
                    media_group_skipped.append(msg_id)
                    skipped += 1
                    continue
                else:
                    # Mark this media group as processed
                    processed_media_groups.add(chat_msg.media_group_id)
                    LOGGER(__name__).info(f"Processing media group {chat_msg.media_group_id} at message {msg_id}")
            
            has_media = bool(chat_msg.media_group_id or chat_msg.media)
            has_text = bool(chat_msg.text or chat_msg.caption)
            
            if not (has_media or has_text):
                skipped += 1
                continue
            
            task = track_task(handle_download(bot, message, url))
            try:
                await task
                downloaded += 1
                
                # If this was a media group, log how many files were in it
                if chat_msg.media_group_id:
                    try:
                        media_group_messages = await chat_msg.get_media_group()
                        LOGGER(__name__).info(f"Media group {chat_msg.media_group_id} contained {len(media_group_messages)} files")
                    except:
                        pass
                        
            except asyncio.CancelledError:
                await loading.delete()
                return await message.reply(
                    f"**❌ Batch canceled** after downloading `{downloaded}` posts."
                )
            except Exception as download_e:
                failed += 1
                deleted_messages.append(msg_id)
                LOGGER(__name__).error(f"Error downloading {url}: {download_e}")
                
        except Exception as e:
            failed += 1
            deleted_messages.append(msg_id)
            LOGGER(__name__).error(f"Error at {url}: {e}")
        
        await asyncio.sleep(3)
    
    await loading.delete()
    
    # Enhanced completion message
    result_message = (
        "**✅ Batch Process Complete!**\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        f"📥 **Downloaded** : `{downloaded}` post(s)\n"
        f"⏭️ **Skipped** : `{skipped}` (no content/deleted/not in topic/media group duplicates)\n"
        f"❌ **Failed** : `{failed}` error(s)"
    )
    
    if start_thread:
        result_message += f"\n📁 **Forum Topic**: {start_thread}"
        result_message += f"\n🎯 **Processed {len(message_ids)} topic messages** (filtered by Telethon)"
    
    if not_in_topic and len(not_in_topic) <= 10:
        result_message += f"\n🚫 **Not in topic**: {', '.join(map(str, not_in_topic))}"
    elif not_in_topic:
        result_message += f"\n🚫 **Not in topic**: {len(not_in_topic)} messages"
    
    if media_group_skipped:
        if len(media_group_skipped) <= 10:
            result_message += f"\n📁 **Media group duplicates skipped**: {', '.join(map(str, media_group_skipped))}"
        else:
            result_message += f"\n📁 **Media group duplicates skipped**: {len(media_group_skipped)} messages"
    
    if deleted_messages and len(deleted_messages) <= 10:
        result_message += f"\n🗑️ **Deleted/Missing**: {', '.join(map(str, deleted_messages))}"
    elif deleted_messages:
        result_message += f"\n🗑️ **Deleted/Missing**: {len(deleted_messages)} messages"
    
    if processed_media_groups:
        result_message += f"\n📦 **Media groups processed**: {len(processed_media_groups)}"
    
    await message.reply(result_message)

@bot.on_message(filters.command("stats") & filters.private)
async def stats(_, message: Message):
    currentTime = get_readable_time(time() - PyroConf.BOT_START_TIME)
    total, used, free = shutil.disk_usage(".")
    total = get_readable_file_size(total)
    used = get_readable_file_size(used)
    free = get_readable_file_size(free)
    sent = get_readable_file_size(psutil.net_io_counters().bytes_sent)
    recv = get_readable_file_size(psutil.net_io_counters().bytes_recv)
    cpuUsage = psutil.cpu_percent(interval=0.5)
    memory = psutil.virtual_memory().percent
    disk = psutil.disk_usage("/").percent
    process = psutil.Process(os.getpid())
    
    stats = (
        "**≧◉◡◉≦ Bot is Up and Running successfully.**\n\n"
        f"**➜ Bot Uptime:** `{currentTime}`\n"
        f"**➜ Total Disk Space:** `{total}`\n"
        f"**➜ Used:** `{used}`\n"
        f"**➜ Free:** `{free}`\n"
        f"**➜ Memory Usage:** `{round(process.memory_info()[0] / 1024**2)} MiB`\n\n"
        f"**➜ Upload:** `{sent}`\n"
        f"**➜ Download:** `{recv}`\n\n"
        f"**➜ CPU:** `{cpuUsage}%` | "
        f"**➜ RAM:** `{memory}%` | "
        f"**➜ DISK:** `{disk}%`"
    )
    await message.reply(stats)

@bot.on_message(filters.command("logs") & filters.private)
async def logs(_, message: Message):
    if os.path.exists("logs.txt"):
        await message.reply_document(document="logs.txt", caption="**Logs**")
    else:
        await message.reply("**Not exists**")

@bot.on_message(filters.command("killall") & filters.private)
async def cancel_all_tasks(_, message: Message):
    cancelled = 0
    for task in list(RUNNING_TASKS):
        if not task.done():
            task.cancel()
            cancelled += 1
    await message.reply(f"**Cancelled {cancelled} running task(s).**")

if __name__ == "__main__":
    try:
        LOGGER(__name__).info("Bot Started!")
        user.start()
        bot.run()
    except KeyboardInterrupt:
        pass
    except Exception as err:
        LOGGER(__name__).error(err)
    finally:
        # Cleanup Telethon connection
        if telethon_handler.client:
            asyncio.run(telethon_handler.disconnect())
        LOGGER(__name__).info("Bot Stopped")