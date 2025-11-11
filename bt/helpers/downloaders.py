# bt/helpers/downloaders.py
# Fixed version with proper cookie handling and MP4 detection

import os
import asyncio
import json
from typing import List, Optional, Tuple
from logger import LOGGER
from helpers.files import get_readable_file_size, get_download_path
from helpers.utils import cmd_exec
from config import PyroConf

async def save_cookies(cookies_text: str) -> bool:
    """
    Save cookies in Netscape format to a file
    Fixed to use tabs instead of spaces
    """
    try:
        # Delete old cookies file if exists
        if os.path.exists(PyroConf.COOKIES_FILE):
            os.remove(PyroConf.COOKIES_FILE)
            LOGGER(__name__).info("Deleted old cookies file")
        
        lines = []
        
        # Add Netscape header if not present
        if not cookies_text.startswith("# Netscape HTTP Cookie File"):
            lines.append("# Netscape HTTP Cookie File")
            lines.append("# This is a generated file! Do not edit.")
            lines.append("")
        
        # Process each line and convert spaces to tabs
        for line in cookies_text.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                lines.append(line)
            else:
                # Split by spaces and rejoin with tabs
                parts = line.split()
                if len(parts) >= 7:
                    # Rejoin with tabs: domain, flag, path, secure, expiration, name, value
                    tab_line = '\t'.join(parts[:6]) + '\t' + ' '.join(parts[6:])
                    lines.append(tab_line)
        
        # Save the file
        with open(PyroConf.COOKIES_FILE, 'w') as f:
            f.write('\n'.join(lines))
            if lines and not lines[-1] == '':
                f.write('\n')
        
        LOGGER(__name__).info("Saved new cookies file")
        return True
    except Exception as e:
        LOGGER(__name__).error(f"Error saving cookies: {e}")
        return False

async def aria2c_download(url: str, download_path: str, progress_callback=None) -> Tuple[bool, str]:
    """
    Download file using aria2c

    """
    try:
        cmd = [
            "aria2c",
            "--load-cookies=/home/user/kolo/bt/cookies.txt",
            "--max-connection-per-server=16",
            "--split=16",
            "--min-split-size=1M",
            "--max-tries=5",
            "--retry-wait=5",
            "--timeout=60",
            "--dir", os.path.dirname(download_path),
            "--out", os.path.basename(download_path),
            "--console-log-level=error",
            "--summary-interval=10",
            "--allow-overwrite=true",
            url
        ]
        
        LOGGER(__name__).info(f"Starting aria2c download: {url}")
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Monitor progress if callback provided
        if progress_callback:
            asyncio.create_task(_monitor_aria2c_progress(process, progress_callback))
        
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            LOGGER(__name__).info(f"Successfully downloaded: {download_path}")
            return True, download_path
        else:
            error_msg = stderr.decode() if stderr else "Unknown error"
            LOGGER(__name__).error(f"aria2c download failed: {error_msg}")
            return False, error_msg
            
    except Exception as e:
        LOGGER(__name__).error(f"Error in aria2c download: {e}")
        return False, str(e)

async def _monitor_aria2c_progress(process, callback):
    """Monitor aria2c download progress"""
    while process.returncode is None:
        await callback()
        await asyncio.sleep(10)

async def ytdlp_download(url: str, download_path: str, use_aria2c: bool = True, progress_message=None) -> Tuple[bool, str, str]:
    """
    Download video using yt-dlp with optional aria2c external downloader
    Returns: (success, file_path, video_title)
    """
    try:
        # First, extract video info including title using yt-dlp's info extraction
        info_cmd = [
            "yt-dlp",
            "--dump-json",  # Get full JSON info instead of just title
            "--no-warnings",
            "--no-playlist"
        ]
        
        # Add cookies if available
        if os.path.exists(PyroConf.COOKIES_FILE):
            info_cmd.extend(["--cookies", PyroConf.COOKIES_FILE])
        
        info_cmd.append(url)
        
        # Get video metadata
        process = await asyncio.create_subprocess_exec(
            *info_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        video_title = None
        if stdout:
            try:
                import json
                video_info = json.loads(stdout.decode())
                video_title = video_info.get('title', '')
                
                # Sanitize title for use as caption
                # if video_title:
                #     # Remove excessive whitespace
                #     video_title = ' '.join(video_title.split())
                #     # Remove problematic characters
                #     video_title = video_title.replace('\n', ' ').replace('\r', ' ')
                #     # Limit length for Telegram caption
                #     if len(video_title) > 200:
                #         video_title = video_title[:197] + "..."
                    
                #     LOGGER(__name__).info(f"Extracted video title: {video_title}")   
                #     # # Also get uploader if available
                #     # uploader = video_info.get('uploader', '')
                #     # if uploader and uploader not in video_title:
                #     #     video_title = f"{video_title} - {uploader}"
                if video_title:
                    # Remove excessive whitespace
                    video_title = ' '.join(video_title.split())
                    # Remove problematic characters
                    video_title = video_title.replace('\n', ' ').replace('\r', ' ')
                    
                    # Get description if available
                    description = video_info.get('description', '')
                    if description:
                        # Clean description similar to title
                        description = ' '.join(description.split())
                        description = description.replace('\n', ' ').replace('\r', ' ')
                        
                        # Add description to title if it's not already included
                        if description not in video_title:
                            video_title = f"{video_title} - {description}"
                    
                    # Limit length for Telegram caption
                    if len(video_title) > 200:
                        video_title = video_title[:197] + "..."
                        
                    LOGGER(__name__).info(f"Extracted video title: {video_title}")
    

                        
            except json.JSONDecodeError:
                LOGGER(__name__).warning("Could not parse video info JSON")
        
        if not video_title:
            LOGGER(__name__).info("Could not extract video title")
        
        # Now download the video
        cmd = [
            "yt-dlp",
            "-o", download_path,
            "--no-warnings",
            "--no-playlist",
            "--prefer-free-formats",
            "--remux-video", "mp4",
            "--no-check-certificates",  # Help with SSL issues
            "--retries", "5",
            "--fragment-retries", "5"
        ]
        
        # Add cookies if available
        if os.path.exists(PyroConf.COOKIES_FILE):
            cmd.extend(["--cookies", PyroConf.COOKIES_FILE])
            LOGGER(__name__).info("Using cookies for yt-dlp download")
        
        # Add aria2c as external downloader if requested
        if use_aria2c:
            cmd.extend([
                "--external-downloader", "aria2c",
                "--external-downloader-args", "aria2c:--max-connection-per-server=16 --split=16 --min-split-size=1M --check-certificate=false"
            ])
            LOGGER(__name__).info("Using aria2c as external downloader")
        
        # Add progress output
        cmd.extend(["--newline", "--progress"])
        
        # Add URL
        cmd.append(url)
        
        LOGGER(__name__).info(f"Starting yt-dlp download: {url}")
        
        if progress_message:
            await progress_message.edit("**📥 Downloading with yt-dlp...**")
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Read output in real-time for progress
        last_percent = 0
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            
            line = line.decode().strip()
            if '[download]' in line and '%' in line and progress_message:
                try:
                    # Extract percentage
                    percent_str = line.split('%')[0].split()[-1]
                    current_percent = float(percent_str)
                    
                    # Only update if percentage changed significantly (reduce spam)
                    if current_percent - last_percent >= 5:
                        if video_title:
                            await progress_message.edit(f"**📥 Downloading: {percent_str}%**\n📹 {video_title[:50]}...")
                        else:
                            await progress_message.edit(f"**📥 Downloading: {percent_str}%**")
                        last_percent = current_percent
                except:
                    pass
        
        await process.wait()
        
        if process.returncode == 0:
            # Find the actual downloaded file
            actual_file = _find_downloaded_file(os.path.dirname(download_path), os.path.basename(download_path))
            if actual_file:
                LOGGER(__name__).info(f"Successfully downloaded: {actual_file}")
                return True, actual_file, video_title
            else:
                return False, "Downloaded file not found", None
        else:
            stderr_data = await process.stderr.read()
            error_msg = stderr_data.decode() if stderr_data else "Unknown error"
            LOGGER(__name__).error(f"yt-dlp download failed: {error_msg}")
            return False, error_msg, None
            
    except Exception as e:
        LOGGER(__name__).error(f"Error in yt-dlp download: {e}")
        return False, str(e), None
        
def _find_downloaded_file(directory: str, base_name: str) -> Optional[str]:
    """Find the actual downloaded file (yt-dlp might change extension)"""
    base_without_ext = os.path.splitext(base_name)[0]
    for file in os.listdir(directory):
        if file.startswith(base_without_ext):
            return os.path.join(directory, file)
    return None

def sanitize_filename(filename: str, max_length: int = 300) -> str:
    """
    Sanitize filename for filesystem compatibility
    """
    # Remove invalid characters
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Remove control characters
    filename = ''.join(char for char in filename if ord(char) >= 32)
    
    # Limit length
    if len(filename) > max_length:
        name, ext = os.path.splitext(filename)
        name = name[:max_length - len(ext) - 3] + "..."
        filename = name + ext
    
    return filename.strip()

def is_video_file(file_path: str) -> bool:
    """Check if file is a video based on extension and mime type"""
    video_extensions = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mpg', '.mpeg', '.3gp'}
    _, ext = os.path.splitext(file_path.lower())
    
    if ext in video_extensions:
        return True
    
    # Also check mime type as fallback
    from mimetypes import guess_type
    mime_type, _ = guess_type(file_path)
    return mime_type and mime_type.startswith('video')

async def split_file_p7zip(file_path: str, max_size_mb: int = 2000, progress_message=None) -> List[str]:
    """
    Split file using p7zip into parts smaller than max_size_mb
    """
    try:
        if progress_message:
            await progress_message.edit("**✂️ Splitting file with 7zip...**")
        
        file_size = os.path.getsize(file_path)
        if file_size <= max_size_mb * 1024 * 1024:
            return []  # No need to split
        
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        output_dir = os.path.dirname(file_path)
        output_base = os.path.join(output_dir, f"{base_name}_part")
        
        # Use 7z to split the file
        cmd = [
            "7z", "a",
            "-v" + str(max_size_mb) + "m",  # Volume size
            "-mx=0",  # No compression (store mode)
            f"{output_base}.7z",
            file_path
        ]
        
        LOGGER(__name__).info(f"Splitting file with 7zip: {file_path}")
        
        stdout, stderr, returncode = await cmd_exec(cmd)
        
        if returncode != 0:
            LOGGER(__name__).error(f"7zip split failed: {stderr}")
            return []
        
        # Find all created parts
        part_files = []
        part_num = 1
        while True:
            part_file = f"{output_base}.7z.{str(part_num).zfill(3)}"
            if os.path.exists(part_file):
                part_files.append(part_file)
                part_num += 1
            else:
                # Check for the main file without number (first part)
                if part_num == 1 and os.path.exists(f"{output_base}.7z"):
                    part_files.append(f"{output_base}.7z")
                break
        
        LOGGER(__name__).info(f"Created {len(part_files)} parts")
        return part_files
        
    except Exception as e:
        LOGGER(__name__).error(f"Error splitting file with p7zip: {e}")
        return []

async def extract_7z_parts(first_part_path: str, output_dir: str) -> Optional[str]:
    """
    Extract 7z split archives
    """
    try:
        cmd = [
            "7z", "x",
            "-y",  # Yes to all prompts
            f"-o{output_dir}",
            first_part_path
        ]
        
        stdout, stderr, returncode = await cmd_exec(cmd)
        
        if returncode == 0:
            # Find extracted file
            for file in os.listdir(output_dir):
                file_path = os.path.join(output_dir, file)
                if os.path.isfile(file_path):
                    return file_path
        return None
        
    except Exception as e:
        LOGGER(__name__).error(f"Error extracting 7z parts: {e}")
        return None