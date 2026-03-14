import json
import os

class ChatMemory:
    def __init__(self, user_id):
        self.file_path = f"chat_memory_{user_id}.json"
    
    def load_history(self):
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r', encoding='utf-8') as file:
                    return json.load(file)
            except (json.JSONDecodeError, FileNotFoundError):
                return []
        return []
    
    def save_history(self, history):
        with open(self.file_path, 'w', encoding='utf-8') as file:
            json.dump(history, file, indent=2)
    
    def get_all(self):
        return self.load_history()
    
    def put_messages(self, messages):
        history = self.load_history()
        history.append(messages)
        self.save_history(history)
    
    def clear_history(self):
        """Clears the chat history to start a new conversation."""
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
        self.save_history([])