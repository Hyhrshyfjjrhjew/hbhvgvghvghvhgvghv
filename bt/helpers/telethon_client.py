# bt/helpers/telethon_client.py
# Add this new file to handle Telethon operations

import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, AuthKeyError, PhoneCodeInvalidError, RPCError
from config import PyroConf
from logger import LOGGER

class TelethonHandler:
    def __init__(self):
        self.client = None
        self.session_string = getattr(PyroConf, 'TELETHON_SESSION', None)
    
    async def create_client(self):
        """Create Telethon client"""
        if self.session_string:
            try:
                LOGGER(__name__).info("Connecting with Telethon session string...")
                self.client = TelegramClient(
                    StringSession(self.session_string), 
                    PyroConf.API_ID, 
                    PyroConf.API_HASH
                )
                await self.client.connect()
                
                if await self.client.is_user_authorized():
                    LOGGER(__name__).info("Successfully connected with Telethon!")
                    return True
                else:
                    LOGGER(__name__).error("Telethon session string is invalid or expired")
                    await self.client.disconnect()
                    return False
            except Exception as e:
                LOGGER(__name__).error(f"Telethon connection failed: {e}")
                return False
        else:
            LOGGER(__name__).error("No TELETHON_SESSION found in config")
            return False
    
    async def get_topic_messages_range(self, chat_id, topic_id, start_msg_id, end_msg_id):
        """
        Get messages from a topic between start_msg_id and end_msg_id (both inclusive)
        Returns list of message IDs that belong to the topic
        """
        if not self.client:
            if not await self.create_client():
                return []
        
        try:
            # Get chat entity
            chat = await self.client.get_entity(chat_id)
            
            LOGGER(__name__).info(f"Getting topic {topic_id} messages from {start_msg_id} to {end_msg_id}")
            
            message_ids = []
            
            # Use iter_messages to get all messages in the topic within the range
            async for message in self.client.iter_messages(
                chat,
                reply_to=topic_id,  # Filter for this topic
                min_id=start_msg_id - 1,  # Get messages after (start_msg_id - 1)
                max_id=end_msg_id + 1,    # Get messages before (end_msg_id + 1)
                reverse=True  # Get in chronological order
            ):
                # Double check the message belongs to topic and is in our range
                if (message.id >= start_msg_id and 
                    message.id <= end_msg_id and 
                    self._message_belongs_to_topic(message, topic_id)):
                    message_ids.append(message.id)
                    LOGGER(__name__).info(f"Found valid topic message: {message.id}")
            
            LOGGER(__name__).info(f"Found {len(message_ids)} messages in topic {topic_id} range")
            return sorted(message_ids)  # Return sorted list
            
        except FloodWaitError as e:
            LOGGER(__name__).warning(f"Rate limit hit. Waiting {e.seconds} seconds...")
            await asyncio.sleep(e.seconds)
            return await self.get_topic_messages_range(chat_id, topic_id, start_msg_id, end_msg_id)
        except Exception as e:
            LOGGER(__name__).error(f"Error getting topic messages: {e}")
            return []
    
    def _message_belongs_to_topic(self, message, topic_id: int) -> bool:
        """Check if a message belongs to a specific forum topic"""
        if not message:
            return False
        
        # Check if this is the topic starter message
        if message.id == topic_id:
            return True
        
        # Check reply_to for topic association
        if hasattr(message, 'reply_to') and message.reply_to:
            if hasattr(message.reply_to, 'reply_to_top_id') and message.reply_to.reply_to_top_id == topic_id:
                return True
            if hasattr(message.reply_to, 'reply_to_msg_id') and message.reply_to.reply_to_msg_id == topic_id:
                return True
        
        return False
    
    async def disconnect(self):
        """Disconnect Telethon client"""
        if self.client:
            await self.client.disconnect()
            self.client = None

# Global instance
telethon_handler = TelethonHandler()