# app.py - Streamlit Application for Pipeline RCA Analysis
import streamlit as st
import asyncio
from datetime import datetime
from pathlib import Path

from agents import Runner, SQLiteSession
from agents.exceptions import InputGuardrailTripwireTriggered, OutputGuardrailTripwireTriggered

from config import Config
from agent_manager import create_rca_agent, detect_pipeline_selection
from utils.logging_config import setup_logging
from utils.rate_limiter import RateLimiter

# Initialize logging
logger = setup_logging()

# Validate configuration
try:
    Config.validate()
except ValueError as e:
    st.error(f"⚠️ Configuration Error: {e}")
    st.stop()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_top_active_pipelines(limit: int = 5):
    """Get top N most active pipelines from the database."""
    try:
        from agent_manager import get_spark_session
        
        spark = get_spark_session()
        if not spark:
            logger.warning("Could not get Spark session for top pipelines")
            return []
        
        query = f"""
        SELECT 
            pipeline_name,
            COUNT(DISTINCT run_id) as run_count,
            MAX(run_timestamp) as last_run
        FROM pipeline_run_metrics
        WHERE DATE(run_timestamp) >= DATE_SUB(CURRENT_DATE(), 7)
        GROUP BY pipeline_name
        ORDER BY COUNT(DISTINCT run_id) DESC
        LIMIT {limit}
        """
        
        df = spark.sql(query)
        results = df.collect()
        
        pipelines = []
        for row in results:
            pipelines.append({
                "name": row.pipeline_name,
                "run_count": int(row.run_count),
                "last_run": str(row.last_run)
            })
        
        logger.info(f"Retrieved {len(pipelines)} top active pipelines")
        return pipelines
        
    except Exception as e:
        logger.error(f"Error getting top pipelines: {e}", exc_info=True)
        return []

# Page configuration
st.set_page_config(
    page_title="Pipeline RCA Analyzer",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .stChatMessage {
        padding: 1rem;
        border-radius: 0.5rem;
    }
    .pipeline-badge {
        background-color: #1f77b4;
        color: white;
        padding: 0.25rem 0.75rem;
        border-radius: 1rem;
        font-size: 0.875rem;
        display: inline-block;
        margin: 0.25rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

if "session_id" not in st.session_state:
    st.session_state.session_id = f"streamlit_{datetime.now().timestamp()}"

if "selected_pipeline" not in st.session_state:
    st.session_state.selected_pipeline = None

if "agent" not in st.session_state:
    st.session_state.agent = create_rca_agent()

if "sdk_session" not in st.session_state:
    # Create data directory if it doesn't exist
    Path("data").mkdir(exist_ok=True)
    
    # Initialize OpenAI Agents SDK Session for automatic conversation history
    st.session_state.sdk_session = SQLiteSession(
        session_id=st.session_state.session_id,
        db_path=Config.SESSION_DB_PATH
    )
    logger.info(f"Session created: {st.session_state.session_id}")

if "rate_limiter" not in st.session_state:
    st.session_state.rate_limiter = RateLimiter(
        max_requests=Config.RATE_LIMIT_REQUESTS,
        window_seconds=Config.RATE_LIMIT_WINDOW
    )

if "messages_display" not in st.session_state:
    # For UI display only - actual memory is in sdk_session
    st.session_state.messages_display = []

if "greeted" not in st.session_state:
    st.session_state.greeted = False

# ============================================================================
# SIDEBAR
# ============================================================================

with st.sidebar:
    st.title("🔍 Pipeline RCA Analyzer")
    st.markdown("---")
    
    # Pipeline selection indicator
    st.subheader("Current Context")
    
    if st.session_state.selected_pipeline:
        st.success(f"✓ Analyzing: **{st.session_state.selected_pipeline}**")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            if st.button("🔍 Show Steps", use_container_width=True, key="show_steps_btn"):
                st.session_state.messages_display.append({
                    "role": "user",
                    "content": f"What steps are in {st.session_state.selected_pipeline}?"
                })
                st.rerun()
        
        with col2:
            if st.button("✖", use_container_width=True, key="clear_pipeline_btn", help="Clear pipeline selection"):
                st.session_state.selected_pipeline = None
                st.session_state.agent = create_rca_agent()
                st.rerun()
    else:
        st.info("💡 No pipeline selected")
        st.caption("Ask me to list pipelines or type a pipeline name (partial names work too!)")
    
    st.markdown("---")
    
    # Quick actions
    st.subheader("Quick Actions")
    
    if st.button("📋 List All Pipelines", use_container_width=True):
        # Add a message to list pipelines
        st.session_state.messages_display.append({
            "role": "user",
            "content": "List all available pipelines"
        })
        st.rerun()
    
    if st.session_state.selected_pipeline:
        if st.button("📊 Show Recent Metrics", use_container_width=True):
            st.session_state.messages_display.append({
                "role": "user",
                "content": f"Show me the latest metrics for {st.session_state.selected_pipeline}"
            })
            st.rerun()
        
        if st.button("🔄 Check Code Changes", use_container_width=True):
            st.session_state.messages_display.append({
                "role": "user",
                "content": f"What code changes happened recently in {st.session_state.selected_pipeline}?"
            })
            st.rerun()
        
        if st.button("📈 Analyze Trends", use_container_width=True):
            st.session_state.messages_display.append({
                "role": "user",
                "content": f"Analyze drop rate trends for {st.session_state.selected_pipeline} over the last 7 days"
            })
            st.rerun()
    
    st.markdown("---")
    
    # Quick Pipeline Selection
    st.subheader("Quick Pipeline Selection")
    
    top_pipelines = get_top_active_pipelines()
    
    if top_pipelines:
        st.caption("Most active pipelines (last 7 days):")
        
        for pipeline in top_pipelines:
            pipeline_name = pipeline["name"]
            
            # Truncate long names for button display
            display_name = pipeline_name if len(pipeline_name) <= 25 else pipeline_name[:22] + "..."
            
            if st.button(
                f"📊 {display_name}",
                key=f"quick_select_{pipeline_name}",
                use_container_width=True,
                help=f"{pipeline_name}\n{pipeline['run_count']} runs in last 7 days"
            ):
                # Add message to chat
                st.session_state.messages_display.append({
                    "role": "user",
                    "content": f"Analyze {pipeline_name}"
                })
                st.rerun()
    else:
        st.caption("💡 No recent pipeline activity found")
        st.caption("Use the chat to list all available pipelines")

    
    # Settings
    st.subheader("Settings")
    st.text(f"Model: {Config.LLM_MODEL}")
    st.text(f"Temperature: {Config.TEMPERATURE}")
    
    st.markdown("---")
    
    # Session info
    st.subheader("Session Info")
    st.text(f"ID: {st.session_state.session_id[:16]}...")
    
    # Get conversation history count
    try:
        session_history = asyncio.run(st.session_state.sdk_session.get_items())
        memory_items = len(session_history)
        st.text(f"Memory Items: {memory_items}")
    except:
        st.text(f"Messages: {len(st.session_state.messages_display)}")
    
    rate_remaining = st.session_state.rate_limiter.get_remaining_requests(
        st.session_state.session_id
    )
    st.text(f"Rate Limit: {rate_remaining}/{Config.RATE_LIMIT_REQUESTS}")
    
    st.markdown("---")
    
    # Memory Management
    st.subheader("Memory Management")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("👁️ View", use_container_width=True):
            try:
                history = asyncio.run(st.session_state.sdk_session.get_items())
                with st.expander("Conversation History", expanded=True):
                    for item in history[-10:]:  # Last 10 items
                        role = getattr(item, 'role', 'unknown')
                        content = getattr(item, 'content', str(item))
                        st.text(f"{role}: {content[:80]}...")
            except Exception as e:
                st.error(f"Error viewing memory: {e}")
    
    with col2:
        if st.button("🗑️ Clear", use_container_width=True):
            asyncio.run(st.session_state.sdk_session.clear_session())
            st.session_state.messages_display = []
            st.session_state.selected_pipeline = None
            st.session_state.agent = create_rca_agent()
            st.session_state.greeted = False
            logger.info(f"Memory cleared for session: {st.session_state.session_id}")
            st.rerun()

# ============================================================================
# MAIN CHAT INTERFACE
# ============================================================================

st.title("🤖 Pipeline RCA Analyzer Bot")
st.caption("Powered by OpenAI Agents SDK - Analyzing Databricks Pipeline Metrics")

# Display greeting message on first load
if not st.session_state.greeted:
    greeting = """👋 **Welcome to the Pipeline RCA Analyzer!**

I help you analyze data pipeline issues by examining metrics and code changes stored in your Delta tables.

**What I can help you with:**
- Investigate why drop rates changed
- Correlate code deployments with performance changes
- Analyze trends across time periods
- Compare different pipeline runs
- Identify which rules or steps are causing issues

**How to get started:**
1. Ask me to list available pipelines, or
2. Tell me which pipeline you want to analyze, or
3. Ask a specific question like "Why did pipeline X drop more records yesterday?"

**Example questions:**
- "List all available pipelines"
- "Show me recent metrics for the customer_data pipeline"
- "What code changes happened in the last week?"
- "Why did the drop rate increase today?"
- "Compare run ABC123 with run DEF456"

I remember our entire conversation, so feel free to ask follow-up questions!"""
    
    st.session_state.messages_display.append({
        "role": "assistant",
        "content": greeting
    })
    st.session_state.greeted = True

# Display chat messages
for message in st.session_state.messages_display:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask me about your pipeline metrics..."):
    
    # Rate limiting check
    if not st.session_state.rate_limiter.check_rate_limit(st.session_state.session_id):
        st.error("⚠️ Rate limit exceeded. Please wait a moment before sending another message.")
        remaining_time = st.session_state.rate_limiter.get_reset_time(st.session_state.session_id)
        st.info(f"Try again in {int(remaining_time)} seconds")
        st.stop()
    
    # Add user message to display
    st.session_state.messages_display.append({
        "role": "user",
        "content": prompt
    })
    
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Generate response
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        
        try:
            # Check for pipeline selection
            detected_pipeline = detect_pipeline_selection(prompt)
            if detected_pipeline and not st.session_state.selected_pipeline:
                st.session_state.selected_pipeline = detected_pipeline
                # Recreate agent with pipeline context
                st.session_state.agent = create_rca_agent(detected_pipeline)
                logger.info(f"Pipeline selected: {detected_pipeline}")
            
            # Run agent with OpenAI Agents SDK
            async def run_agent():
                try:
                    result = await Runner.run(
                        starting_agent=st.session_state.agent,
                        input=prompt,
                        session=st.session_state.sdk_session
                    )
                    return result.final_output
                
                except InputGuardrailTripwireTriggered as e:
                    logger.warning(f"Input guardrail triggered: {e}")
                    reason = e.guardrail_result.output.output_info.get("reason", "unknown")
                    
                    if reason in ["prohibited_pattern", "code_generation_request"]:
                        return "I can only help with pipeline analysis and RCA. Please ask about pipeline metrics, code changes, or drop rate analysis."
                    elif reason == "malicious_command":
                        return "I cannot execute commands or make system changes. I'm designed to analyze and investigate issues through:\n\n• Examining logs and metrics\n• Identifying correlations and patterns\n• Mapping dependencies\n• Providing insights for root cause analysis\n\nHow can I help you analyze a technical issue?"
                    elif reason == "input_too_long":
                        length = e.guardrail_result.output.output_info.get("length", 0)
                        return f"Your input is too long ({length} characters). Please provide a more concise question (max {Config.MAX_INPUT_LENGTH} characters)."
                    elif reason == "empty_input":
                        return "I didn't receive any input. How can I help you analyze your pipelines?"
                    else:
                        return "I'm unable to process your request. Please ask about pipeline metrics or analysis."
                    
                except OutputGuardrailTripwireTriggered as e:
                    logger.warning(f"Output guardrail triggered: {e}")
                    reason = e.guardrail_result.output.output_info.get("reason", "unknown")
                    
                    if reason == "empty_output":
                        return "I'm unable to process your request. Please describe the technical issue you'd like me to analyze."
                    elif reason == "sensitive_data":
                        return "Output blocked: Sensitive information detected."             
                    elif reason == "output_too_long":
                        return "The response was too long. Please try asking for a more specific subset of the data."
                    else:
                        return "I encountered an issue generating the response. Please try rephrasing your question."
                
                except Exception as e:
                    logger.error(f"Error running agent: {e}", exc_info=True)
                    return f"I encountered an error: {str(e)}\n\nPlease try again or ask a different question."
            
            # Show loading indicator
            with st.spinner("Analyzing pipeline data..."):
                response = asyncio.run(run_agent())
            
            # Display response
            message_placeholder.markdown(response)
            
            # Add to display history
            st.session_state.messages_display.append({
                "role": "assistant",
                "content": response
            })
            
            logger.info(f"Session {st.session_state.session_id}: Successfully processed message")
            
        except Exception as e:
            error_msg = "I apologize, but I encountered an error processing your request. Please try again or rephrase your question."
            message_placeholder.error(error_msg)
            logger.error(f"Error in session {st.session_state.session_id}: {str(e)}", exc_info=True)
            
            st.session_state.messages_display.append({
                "role": "assistant",
                "content": error_msg
            })

# Footer
st.markdown("---")
col1, col2, col3 = st.columns(3)

with col1:
    st.caption(f"📊 {Config.APP_NAME} v{Config.APP_VERSION}")

with col2:
    st.caption("🔧 Powered by OpenAI Agents SDK")

with col3:
    st.caption("💾 Data stored in Databricks Delta Tables")