# utils/rate_limiter.py
"""Rate limiting implementation using token bucket algorithm"""

import time
from collections import defaultdict
from threading import Lock
from typing import Dict, List
from utils.logging_config import get_logger

logger = get_logger(__name__)

class RateLimiter:
    """
    Token bucket rate limiter for preventing abuse and DoS attacks
    
    Each identifier (e.g., session_id) gets a bucket with max_requests tokens.
    Tokens are consumed on each request and replenished over time.
    """
    
    def __init__(self, max_requests: int = 20, window_seconds: int = 60):
        """
        Initialize rate limiter
        
        Args:
            max_requests: Maximum number of requests allowed in the time window
            window_seconds: Time window in seconds
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: Dict[str, List[float]] = defaultdict(list)
        self.lock = Lock()
        
        logger.info(f"RateLimiter initialized: {max_requests} requests per {window_seconds}s")
    
    def check_rate_limit(self, identifier: str) -> bool:
        """
        Check if request is within rate limit
        
        Args:
            identifier: Unique identifier (e.g., session_id, user_id, IP)
        
        Returns:
            True if request is allowed, False if rate limit exceeded
        """
        
        with self.lock:
            current_time = time.time()
            
            # Clean up old requests outside the time window
            self.requests[identifier] = [
                req_time for req_time in self.requests[identifier]
                if current_time - req_time < self.window_seconds
            ]
            
            # Check if limit exceeded
            if len(self.requests[identifier]) >= self.max_requests:
                logger.warning(
                    f"Rate limit exceeded for {identifier}: "
                    f"{len(self.requests[identifier])}/{self.max_requests} requests"
                )
                return False
            
            # Add current request timestamp
            self.requests[identifier].append(current_time)
            
            logger.debug(
                f"Rate limit check passed for {identifier}: "
                f"{len(self.requests[identifier])}/{self.max_requests} requests"
            )
            
            return True
    
    def get_remaining_requests(self, identifier: str) -> int:
        """
        Get remaining requests for identifier
        
        Args:
            identifier: Unique identifier
        
        Returns:
            Number of remaining requests in current window
        """
        
        with self.lock:
            current_time = time.time()
            
            # Clean old requests
            self.requests[identifier] = [
                req_time for req_time in self.requests[identifier]
                if current_time - req_time < self.window_seconds
            ]
            
            remaining = max(0, self.max_requests - len(self.requests[identifier]))
            return remaining
    
    def get_reset_time(self, identifier: str) -> float:
        """
        Get time (in seconds) until rate limit resets for identifier
        
        Args:
            identifier: Unique identifier
        
        Returns:
            Seconds until oldest request expires (rate limit resets)
        """
        
        with self.lock:
            if not self.requests[identifier]:
                return 0.0
            
            current_time = time.time()
            oldest_request = min(self.requests[identifier])
            reset_time = max(0, self.window_seconds - (current_time - oldest_request))
            
            return reset_time
    
    def reset_limit(self, identifier: str):
        """
        Reset rate limit for specific identifier (admin/testing use)
        
        Args:
            identifier: Unique identifier to reset
        """
        
        with self.lock:
            if identifier in self.requests:
                del self.requests[identifier]
                logger.info(f"Rate limit reset for {identifier}")
    
    def get_stats(self) -> Dict:
        """
        Get rate limiter statistics
        
        Returns:
            Dictionary with statistics
        """
        
        with self.lock:
            current_time = time.time()
            active_identifiers = 0
            total_requests = 0
            
            for identifier, requests in list(self.requests.items()):
                # Clean old requests
                valid_requests = [
                    req for req in requests
                    if current_time - req < self.window_seconds
                ]
                
                if valid_requests:
                    active_identifiers += 1
                    total_requests += len(valid_requests)
                else:
                    # Remove empty identifiers
                    del self.requests[identifier]
            
            return {
                "max_requests_per_window": self.max_requests,
                "window_seconds": self.window_seconds,
                "active_identifiers": active_identifiers,
                "total_active_requests": total_requests
            }