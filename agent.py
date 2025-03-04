from __future__ import annotations
import os
import json

# Import compatibility layer first

# Now import LiveKit modules
from dotenv import load_dotenv
from livekit.agents import (
    AutoSubscribe,
    JobContext,
    WorkerOptions,
    cli,
    llm
)
from livekit.agents.multimodal import MultimodalAgent
from livekit.plugins import openai

from api import AssistantFnc, BevDetails
from prompts import (
    WELCOME_MESSAGE, 
    INSTRUCTIONS, 
    LOOKUP_BEV_MESSAGE,
    CATEGORY_HELP_MESSAGE,
    get_subcategory_help,
    CATEGORIES
)

load_dotenv()

async def entrypoint(ctx: JobContext):
    await ctx.connect(auto_subscribe=AutoSubscribe.SUBSCRIBE_ALL)
    await ctx.wait_for_participant()
    
    model = openai.realtime.RealtimeModel(
        instructions=INSTRUCTIONS,
        voice="shimmer",
        temperature=0.6,
        modalities=["audio", "text"]
    )
    bev_fnc = AssistantFnc()
    bev_agent = MultimodalAgent(model=model, fnc_ctx=bev_fnc)
    bev_agent.start(ctx.room)
    
    session = model.sessions[0]
    session.conversation.item.create(
        llm.ChatMessage(
            role="assistant",
            content=WELCOME_MESSAGE
        )
    )
    session.response.create()
    
    @session.on("user_speech_committed")
    def on_user_speech_committed(msg: llm.ChatMessage):
        if isinstance(msg.content, list):
            msg.content = "\n".join("[image]" if isinstance(x, llm.ChatImage) else x for x in msg)
            
        if bev_fnc.has_bev():
            handle_query(msg)
        else:
            lookup_bev(msg)
        
    def lookup_bev(msg: llm.ChatMessage):
        session.conversation.item.create(
            llm.ChatMessage(
                role="system",
                content=LOOKUP_BEV_MESSAGE(msg)
            )
        )
        session.response.create()
        
    def handle_query(msg: llm.ChatMessage):
        content = msg.content.lower()
        
        # Check inventory after any interaction
        inventory_status = bev_fnc.check_inventory_levels()
        if inventory_status:
            if "yes" in content.lower() and "notify" in content.lower():
                notification_response = "I've notified Brian and Chris about the low inventory. They will handle the reorder soon."
                session.conversation.item.create(
                    llm.ChatMessage(
                        role="assistant",
                        content=notification_response
                    )
                )
                session.response.create()
                return
            
            session.conversation.item.create(
                llm.ChatMessage(
                    role="assistant",
                    content=inventory_status
                )
            )
            session.response.create()
            return

        # Handle order processing
        if "process" in content and ("order" in content or "transaction" in content):
            # Extract items from recent conversation
            items = []
            if hasattr(bev_fnc, "_bev_details") and bev_fnc._bev_details.get(BevDetails.ID):
                items.append({
                    "id": bev_fnc._bev_details[BevDetails.ID],
                    "quantity": 1
                })
            
            # Create items JSON
            items_json = json.dumps(items)
            
            # Process the transaction
            try:
                result = bev_fnc.create_transaction(items_json, "cash")
                session.conversation.item.create(
                    llm.ChatMessage(
                        role="assistant",
                        content=result
                    )
                )
                session.response.create()
                return
            except Exception as e:
                error_msg = f"Sorry, there was an error processing the transaction: {str(e)}"
                session.conversation.item.create(
                    llm.ChatMessage(
                        role="assistant",
                        content=error_msg
                    )
                )
                session.response.create()
                return

        # Original query handling
        if "categories" in content or "menu" in content:
            response_content = CATEGORY_HELP_MESSAGE
        elif "category" in content and "types" in content:
            # Extract category name from message and get subcategory help
            # This is a simple example - you might want more sophisticated parsing
            for category in CATEGORIES.keys():
                if (category.lower() in content):
                    response_content = get_subcategory_help(category)
                    break
            else:
                response_content = "I'm not sure which category you're asking about. " + CATEGORY_HELP_MESSAGE
        else:
            # Handle other queries as before
            session.conversation.item.create(
                llm.ChatMessage(
                    role="user",
                    content=msg.content
                )
            )
            return session.response.create()

        # Send category-related response
        session.conversation.item.create(
            llm.ChatMessage(
                role="assistant",
                content=response_content
            )
        )
        session.response.create()
    
if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint))