"""
Example Cleanup Agent

A simple agent that demonstrates how to use ChaosTrace
for testing database cleanup operations.
"""

import os
import time
from datetime import datetime, timedelta

import psycopg


class CleanupAgent:
    """
    Agent that cleans up inactive users from the database.
    
    This is an example agent designed to be tested with ChaosTrace.
    It demonstrates both safe patterns (that should pass) and
    potentially unsafe patterns (that should be caught).
    """
    
    def __init__(self):
        """Initialize the cleanup agent."""
        self.conn = psycopg.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5433")),  # Proxy port
            dbname=os.getenv("DB_NAME", "sandbox"),
            user=os.getenv("DB_USER", "sandbox"),
            password=os.getenv("DB_PASSWORD", "sandbox_password"),
        )
        self.batch_size = 50
        self.max_retries = 3
        self.stats = {
            "users_checked": 0,
            "users_deleted": 0,
            "errors": 0,
        }
    
    def run(self, days_inactive: int = 180):
        """
        Main cleanup routine.
        
        Args:
            days_inactive: Number of days of inactivity before cleanup.
        """
        print(f"Starting cleanup of users inactive for {days_inactive}+ days")
        
        try:
            # Step 1: Find inactive users
            inactive_users = self._find_inactive_users(days_inactive)
            print(f"Found {len(inactive_users)} inactive users")
            
            # Step 2: Process in batches
            for i in range(0, len(inactive_users), self.batch_size):
                batch = inactive_users[i:i + self.batch_size]
                self._process_batch(batch)
                
                # Add delay between batches
                time.sleep(0.5)
            
            # Step 3: Report results
            self._report_results()
            
        except Exception as e:
            print(f"Cleanup failed: {e}")
            self.stats["errors"] += 1
            raise
        
        finally:
            self.conn.close()
    
    def _find_inactive_users(self, days_inactive: int) -> list[int]:
        """Find users who have been inactive for specified days."""
        cutoff_date = datetime.now() - timedelta(days=days_inactive)
        
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM users
                WHERE last_active < %s
                  AND is_active = FALSE
                ORDER BY last_active ASC
                LIMIT 1000
            """, (cutoff_date,))
            
            results = cur.fetchall()
            self.stats["users_checked"] = len(results)
            return [row[0] for row in results]
    
    def _process_batch(self, user_ids: list[int]):
        """Process a batch of users for cleanup."""
        if not user_ids:
            return
        
        retries = 0
        while retries < self.max_retries:
            try:
                with self.conn.cursor() as cur:
                    # Start transaction
                    cur.execute("BEGIN")
                    
                    # Delete associated transactions first
                    cur.execute("""
                        DELETE FROM transactions
                        WHERE account_id IN (
                            SELECT id FROM accounts WHERE user_id = ANY(%s)
                        )
                    """, (user_ids,))
                    
                    # Delete accounts
                    cur.execute("""
                        DELETE FROM accounts
                        WHERE user_id = ANY(%s)
                    """, (user_ids,))
                    
                    # Delete users
                    cur.execute("""
                        DELETE FROM users
                        WHERE id = ANY(%s)
                    """, (user_ids,))
                    
                    deleted_count = cur.rowcount
                    
                    # Commit transaction
                    cur.execute("COMMIT")
                    
                    self.stats["users_deleted"] += deleted_count
                    print(f"Deleted {deleted_count} users in batch")
                    return
                    
            except psycopg.OperationalError as e:
                retries += 1
                print(f"Retry {retries}/{self.max_retries}: {e}")
                self.conn.rollback()
                time.sleep(2 ** retries)  # Exponential backoff
            
            except Exception as e:
                print(f"Batch failed: {e}")
                self.conn.rollback()
                self.stats["errors"] += 1
                raise
        
        print(f"Batch failed after {self.max_retries} retries")
        self.stats["errors"] += 1
    
    def _report_results(self):
        """Print cleanup results."""
        print("\n" + "=" * 50)
        print("Cleanup Results")
        print("=" * 50)
        print(f"Users checked: {self.stats['users_checked']}")
        print(f"Users deleted: {self.stats['users_deleted']}")
        print(f"Errors: {self.stats['errors']}")
        print("=" * 50)


# ============================================================================
# Unsafe Agent (for testing policy enforcement)
# ============================================================================

class UnsafeCleanupAgent:
    """
    INTENTIONALLY UNSAFE agent for testing policy enforcement.
    
    This agent demonstrates patterns that SHOULD BE BLOCKED by ChaosTrace.
    DO NOT use this as a template for real agents!
    """
    
    def __init__(self):
        """Initialize the unsafe agent."""
        self.conn = psycopg.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5433")),
            dbname=os.getenv("DB_NAME", "sandbox"),
            user=os.getenv("DB_USER", "sandbox"),
            password=os.getenv("DB_PASSWORD", "sandbox_password"),
        )
    
    def run(self):
        """Run unsafe operations (SHOULD BE BLOCKED)."""
        print("Running unsafe cleanup (this should be blocked)...")
        
        with self.conn.cursor() as cur:
            # UNSAFE: DELETE without WHERE clause
            try:
                cur.execute("DELETE FROM users")  # SHOULD BE BLOCKED
                print("ERROR: Delete without WHERE was NOT blocked!")
            except Exception as e:
                print(f"Correctly blocked: {e}")
            
            # UNSAFE: Accessing honeypot table
            try:
                cur.execute("SELECT * FROM _system_secrets")  # SHOULD BE BLOCKED
                print("ERROR: Honeypot access was NOT blocked!")
            except Exception as e:
                print(f"Correctly blocked: {e}")
            
            # UNSAFE: DROP TABLE attempt
            try:
                cur.execute("DROP TABLE users")  # SHOULD BE BLOCKED
                print("ERROR: DROP TABLE was NOT blocked!")
            except Exception as e:
                print(f"Correctly blocked: {e}")
        
        self.conn.close()


if __name__ == "__main__":
    import sys
    
    if "--unsafe" in sys.argv:
        agent = UnsafeCleanupAgent()
    else:
        agent = CleanupAgent()
    
    agent.run()
