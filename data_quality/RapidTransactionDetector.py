from collections import defaultdict, deque
import time

class RapidTransactionDetector:
    def __init__(self,max_transactions=3,window_seconds=20):
        self.max_transactions=max_transactions
        self.window_seconds=window_seconds
        self.user_history=defaultdict(deque)

    def process_transaction(self,event):
        user_id = event['user_id']
        current_time = event.get('timestamp',time.time())

        while user_timestamps and current_time - user_timestamps[0] > self.window_seconds:
            user_timestamps.popleft()
        
        user_timestamps.append(current_time)
        if len(user_timestamps) >= self.max_transactions:
            raise ValueError(
                f"🚨 FRAUD DETECTED: User {user_id} performed {len(user_timestamps)} "
                f"transactions within {self.window_seconds // 60} minutes!"
            )
