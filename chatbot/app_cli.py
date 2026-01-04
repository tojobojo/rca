#!/usr/bin/env python3
"""
Databricks-compatible CLI for Pipeline RCA Analyzer Bot
Works in Databricks notebooks and standard terminals
"""

import subprocess
import sys
import uuid
import os
import asyncio
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------
# Package installation
# ---------------------------------------------------------------------

def install_packages():
    """Install required packages if not already installed"""
    try:
        import agents
        return
    except ImportError:
        pass

    packages = [
        "typing_extensions>=4.9.0",
        "openai-agents>=0.6.4",     # Latest: v0.6.4 (Dec 2025)
        "pydantic>=2.12.5",         # Latest: v2.12.5 (Nov 26, 2025)
        "litellm>=1.80.11",         # Latest: v1.80.11 (Dec 22, 2025)
        "structlog>=25.5.0"         # Latest: v25.5.0 (Oct 27, 2025)
        
    ]

    print("Installing required packages...")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--upgrade"] + packages
    )
    print("Installation complete!")

install_packages()

# ---------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------

import os

# fixing asyncio logs from litellm
os.environ["LITELLM_LOGGING"] = "False"
os.environ["LITELLM_DISABLE_LOGGING"] = "True"

from agents import Runner, SQLiteSession
from agents.exceptions import (
    InputGuardrailTripwireTriggered,
    OutputGuardrailTripwireTriggered,
)

from config import Config
from agent_manager import create_rca_agent, detect_pipeline_selection
from utils.logging_config import setup_logging
from utils.rate_limiter import RateLimiter

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

import logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = setup_logging()

# ---------------------------------------------------------------------
# Async runner helper for Databricks compatibility
# ---------------------------------------------------------------------

import threading
import concurrent.futures

def run_agent_sync(coro):
    """
    Safely run async agent code from sync CLI,
    even when an event loop is already running (Databricks notebooks).
    """
    def runner():
        return asyncio.run(coro)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(runner)
        return future.result()

# ---------------------------------------------------------------------
# CLI Implementation
# ---------------------------------------------------------------------

class PipelineRCACLI:
    """Command-line interface for Pipeline RCA Analysis"""
    
    def __init__(self):
        """Initialize the CLI with agent and session"""
        Config.validate()

        self.session_id = str(uuid.uuid4())
        self.selected_pipeline = None
        self.agent = create_rca_agent()

        Path("data").mkdir(exist_ok=True)

        self.sdk_session = SQLiteSession(
            session_id=self.session_id,
            db_path=Config.SESSION_DB_PATH,
        )

        self.rate_limiter = RateLimiter(
            max_requests=Config.RATE_LIMIT_REQUESTS,
            window_seconds=Config.RATE_LIMIT_WINDOW,
        )

        logger.info(f"Pipeline RCA CLI initialized (session_id={self.session_id[:16]}...)")

    # -----------------------------------------------------------------

    async def _process_user_input_async(self, user_input: str) -> str:
        """Process user input asynchronously through the agent"""
        
        # Rate limiting check
        if not self.rate_limiter.check_rate_limit(self.session_id):
            wait = int(self.rate_limiter.get_reset_time(self.session_id))
            return f"⚠️ Rate limit exceeded. Try again in {wait} seconds."

        # Check for pipeline selection
        detected_pipeline = detect_pipeline_selection(user_input)
        if detected_pipeline and not self.selected_pipeline:
            self.selected_pipeline = detected_pipeline
            self.agent = create_rca_agent(detected_pipeline)
            logger.info(f"✓ Pipeline selected: {detected_pipeline}")

        try:
            # Run the agent with full conversation context
            result = await Runner.run(
                starting_agent=self.agent,
                input=user_input,
                session=self.sdk_session,
            )
            return result.final_output

        except InputGuardrailTripwireTriggered as e:
            logger.warning(f"Input guardrail triggered: {e}")
            reason = e.guardrail_result.output.output_info.get("reason", "unknown")
            
            if reason == "prohibited_pattern":
                return "I can only help with pipeline analysis. Please ask about metrics, code changes, or drop rates."
            elif reason == "input_too_long":
                return f"Input too long. Please keep it under {Config.MAX_INPUT_LENGTH} characters."
            else:
                return "Unable to process input. Please rephrase your question."

        except OutputGuardrailTripwireTriggered as e:
            logger.warning(f"Output guardrail triggered: {e}")
            return "Response blocked by guardrails. Please try a different question."

        except Exception as e:
            logger.error(f"Agent error: {e}", exc_info=True)
            return f"Error: {str(e)}\n\nPlease try again or rephrase your question."

    # -----------------------------------------------------------------

    def print_welcome(self):
        """Print welcome message"""
        print("\n" + "="*70)
        print("🔍 PIPELINE RCA ANALYZER - Command Line Interface")
        print("="*70)
        print("\nI help you analyze data pipeline issues using metrics from Delta tables.")
        print("\n📊 What I can help with:")
        print("  • Investigate drop rate changes")
        print("  • Analyze code change impacts")
        print("  • Compare pipeline runs")
        print("  • Identify trending issues")
        print("\n💡 Example questions:")
        print('  • "List all available pipelines"')
        print('  • "Show recent metrics for customer_pipeline"')
        print('  • "What code changes happened in the last week?"')
        print('  • "Why did drop rates increase yesterday?"')
        print('  • "Compare run ABC123 with run DEF456"')
        print("\n📝 Commands:")
        print("  • Type your question and press Enter")
        print("  • Type 'quit' or 'exit' to stop")
        print("  • Type 'clear' to reset conversation")
        print("  • Type 'status' to see session info")
        print("\n" + "="*70 + "\n")

    def print_status(self):
        """Print current session status"""
        print("\n" + "-"*70)
        print("📊 SESSION STATUS")
        print("-"*70)
        print(f"Session ID: {self.session_id[:16]}...")
        print(f"Selected Pipeline: {self.selected_pipeline or 'None'}")
        
        try:
            history = run_agent_sync(self.sdk_session.get_items())
            print(f"Memory Items: {len(history)}")
        except:
            print("Memory Items: Unknown")
        
        rate_remaining = self.rate_limiter.get_remaining_requests(self.session_id)
        print(f"Rate Limit: {rate_remaining}/{Config.RATE_LIMIT_REQUESTS} requests remaining")
        print("-"*70 + "\n")

    def clear_session(self):
        """Clear conversation history"""
        run_agent_sync(self.sdk_session.clear_session())
        self.selected_pipeline = None
        self.agent = create_rca_agent()
        logger.info("✓ Session cleared")
        print("\n✓ Conversation history cleared!\n")

    # -----------------------------------------------------------------

    def run(self):
        """Main synchronous CLI loop (Databricks-friendly)"""
        
        self.print_welcome()

        while True:
            try:
                # Get user input
                if self.selected_pipeline:
                    prompt = f"\n[{self.selected_pipeline}] You: "
                else:
                    prompt = "\nYou: "
                
                user_input = input(prompt).strip()

                if not user_input:
                    continue

                # Handle special commands
                if user_input.lower() in {"quit", "exit", "q"}:
                    print("\n👋 Thank you for using Pipeline RCA Analyzer!")
                    logger.info("CLI session ended")
                    break
                
                if user_input.lower() == "clear":
                    self.clear_session()
                    continue
                
                if user_input.lower() == "status":
                    self.print_status()
                    continue

                # Process through agent
                logger.debug(f"Processing user input: {user_input[:50]}...")
                
                print("\n🤖 Assistant: ", end="", flush=True)
                print("Analyzing...", end="", flush=True)
                
                response = run_agent_sync(
                    self._process_user_input_async(user_input)
                )
                
                # Clear "Analyzing..." and print response
                print("\r🤖 Assistant: " + " "*20 + "\r🤖 Assistant: ", end="")
                print(response)

            except KeyboardInterrupt:
                print("\n\n👋 Session interrupted. Goodbye!")
                logger.info("CLI session interrupted")
                break

            except EOFError:
                print("\n\n👋 End of input. Goodbye!")
                logger.info("CLI session ended (EOF)")
                break

            except Exception as e:
                logger.error(f"CLI error: {e}", exc_info=True)
                print(f"\n❌ Error: {str(e)}")
                print("Please try again or type 'quit' to exit.\n")

# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------

def main():
    """Main entry point for the CLI"""
    try:
        cli = PipelineRCACLI()
        cli.run()
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"\n❌ Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()