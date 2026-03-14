from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, Any
from Rag_agent.services.chat_service import get_query_response

router = APIRouter()

class ChatRequest(BaseModel):
    message: str

class ChatResponse(BaseModel):
    answer: str
    status_code: int
    error: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None

@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Endpoint to receive a chat message and return a response.

    Args:
        request (ChatRequest): The request body containing the chat message.

    Returns:
        ChatResponse: The response body containing the chat response.
    """
    print("ml-pipeline chat api invoked")
    try:
        response = await get_query_response(request.message)
        print(f"Chat service response to chat  API in ML: {response}")

        return ChatResponse(answer=response, status_code=200, metadata={"info": "This is mock metadata"})
    except Exception as e:
        print(f"Error using ML chat service: {e}")
        return ChatResponse(answer="", status_code=500, error=str(e))